# Laporan UAS Sistem Terdistribusi
## Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Kontrol Konkurensi

**Nama:** Riza Luthfie Rusmana
**NIM:** 11231088
**Mata Kuliah:** Sistem Paralel dan Terdistribusi
**Repositori:** (isi link GitHub Anda)
**Video Demo:** (isi link YouTube unlisted/public)

---

## 1. Ringkasan Sistem dan Arsitektur

Sistem ini merupakan log aggregator terdistribusi yang dibangun di atas pola publish–subscribe dan berjalan sepenuhnya melalui Docker Compose pada jaringan lokal tanpa ketergantungan layanan eksternal publik. Tujuan utamanya adalah menerima aliran event dari sumber yang tidak andal, lalu memprosesnya secara idempoten sehingga setiap event yang identik hanya tercatat satu kali meskipun diterima berulang kali. Sistem terdiri atas empat layanan yang tanggung jawabnya terpisah. Layanan aggregator menyediakan antarmuka HTTP sekaligus menjalankan kumpulan worker yang mengonsumsi event dari antrian. Layanan publisher bertindak sebagai simulator sumber event yang sengaja mengirim duplikat untuk menguji ketahanan dedup. Layanan broker menggunakan Redis sebagai antrian pesan internal, sedangkan layanan storage menggunakan PostgreSQL sebagai dedup store persisten.

Aliran data berjalan dalam dua jalur. Jalur sinkron melalui endpoint `POST /publish` memproses event langsung ke basis data dan bersifat deterministik, sehingga cocok untuk verifikasi manual. Jalur asinkron melalui endpoint `POST /enqueue` atau publisher mendorong event ke antrian Redis, yang kemudian dikonsumsi oleh empat worker secara paralel. Kedua jalur memakai satu fungsi inti pemrosesan yang sama, sehingga jaminan idempotensi konsisten di mana pun event masuk. Pemisahan layanan ini mencerminkan model arsitektur yang dibahas pada Bab 2 buku acuan, dan pemanfaatan antrian pesan sebagai perantara mengikuti paradigma komunikasi tak langsung pada Bab 6 (Coulouris dkk., 2012).

### Diagram Arsitektur

```
              +-------------+        events_queue
 publisher -->|   broker    |<----------------------+
              | (Redis 7)   |                       |
              +-------------+                        |
                    ^                                |
                    | LPUSH                          | BRPOP (paralel)
                    |                          +-----+------------------+
 curl/klien --------+----HTTP /enqueue-------> |     aggregator         |
            \                                  |  (FastAPI + 4 worker)  |
             \------HTTP /publish-----------> |                        |
                                               +-----------+------------+
                                                           | transaksi
                                                           v
                                                 +---------------------+
                                                 |  storage (Postgres) |
                                                 |  UNIQUE(topic,      |
                                                 |        event_id)    |
                                                 +---------------------+
                                                 volume: pg_data (persisten)
```

### Model Event dan API

Setiap event berbentuk JSON dengan field `topic`, `event_id`, `timestamp` (ISO 8601), `source`, dan `payload`. Endpoint yang tersedia adalah `POST /publish` untuk pemrosesan sinkron tunggal maupun batch, `POST /enqueue` untuk mendorong event ke antrian, `GET /events` untuk menampilkan event unik yang telah diproses, `GET /stats` untuk metrik agregat, dan `GET /healthz` untuk liveness probe.

---

## 2. Keputusan Desain

### 2.1 Idempotency

Idempotensi dicapai dengan menjadikan pasangan `(topic, event_id)` sebagai kunci unik. Saat sebuah event diproses, sistem mencoba menyisipkannya ke tabel `processed_events` menggunakan perintah `INSERT ... ON CONFLICT DO NOTHING`. Bila pasangan kunci sudah ada, penyisipan diabaikan dan event dihitung sebagai duplikat. Dengan demikian, memproses event yang sama satu kali atau seratus kali menghasilkan keadaan basis data yang identik. Sifat ini penting karena sistem menjamin pengiriman at-least-once, sehingga duplikasi adalah hal yang diharapkan dan harus ditangani konsumen, bukan dianggap anomali (Coulouris dkk., 2012, Bab 5).

### 2.2 Dedup Store

Dedup store dipilih berbasis PostgreSQL karena batasan unik di tingkat basis data memberikan jaminan atomik yang tidak bergantung pada koordinasi di tingkat aplikasi. Tabel `processed_events` mendefinisikan `UNIQUE (topic, event_id)`, sehingga dua worker yang berusaha menyisipkan kunci sama secara bersamaan tetap menghasilkan tepat satu baris. Store ini disimpan pada named volume `pg_data` agar data bertahan meskipun kontainer dihapus.

### 2.3 Transaksi dan Konkurensi

Setiap pemrosesan event dibungkus dalam satu transaksi. Di dalam transaksi tersebut, penghitung statistik dinaikkan dan penyisipan event dilakukan. Pendekatan ini memastikan statistik dan data event selalu konsisten satu sama lain, sesuai properti atomicity pada transaksi (Coulouris dkk., 2012, Bab 16). Empat worker berjalan paralel mengonsumsi antrian, dan kebenaran tetap terjaga karena batasan unik menyelesaikan konflik secara atomik tanpa memerlukan kunci eksplisit yang dikelola aplikasi.

### 2.4 Isolation Level

Sistem menggunakan tingkat isolasi READ COMMITTED, bawaan PostgreSQL. Pemilihan ini disengaja karena jaminan dedup tidak bergantung pada pola baca-lalu-tulis yang rentan terhadap lost update, melainkan pada batasan unik yang bersifat atomik. Penghitung statistik dinaikkan dengan pernyataan `SET kolom = kolom + 1` yang mengambil row-level lock, sehingga bebas dari lost update meski banyak worker bekerja serentak. Tingkat SERIALIZABLE tidak diperlukan dan hanya akan menambah biaya berupa serialization failure yang menuntut mekanisme retry (The PostgreSQL Global Development Group, 2024).

### 2.5 Ordering

Sistem tidak mewajibkan total ordering karena tujuan aggregator adalah mencatat keunikan event, bukan merekonstruksi urutan kausal yang ketat. Setiap event membawa `timestamp`, dan basis data memberi `id` monoton sebagai urutan kedatangan praktis. Pendekatan ini menerima kemungkinan event tiba out-of-order, yang tidak memengaruhi kebenaran dedup. Pembahasan keterbatasan waktu dan urutan ini berakar pada Bab 14 buku acuan (Coulouris dkk., 2012).

### 2.6 Retry dan Ketahanan

Publisher menerapkan model at-least-once dengan mengirim ulang sebagian event. Worker yang gagal memproses satu event akan menangani galat tanpa menghentikan loop, sehingga event lain tetap diproses. Karena dedup store bersifat persisten, proses ulang setelah kontainer dijalankan kembali tidak akan menyebabkan pemrosesan ganda.

---

## 3. Analisis Performa dan Hasil Uji Konkurensi

Pengujian beban dilakukan dengan mendorong 20.000 event yang mengandung 30% duplikat melalui jalur antrian, sesuai ketentuan minimum. Seluruh metrik berikut diperoleh dari skrip `perf/loadtest.py` pada lingkungan Docker Desktop di Windows.

### 3.1 Throughput dan Dedup

| Metrik | Nilai |
|---|---|
| Total event diterima | 20.000 |
| Event unik diproses | 14.000 |
| Duplikat ditolak | 6.000 |
| Duplicate rate aktual | 30,0% |
| Throughput ingest | 12.198 event/detik |
| Waktu memproses antrian | 93,81 detik |
| Throughput pemrosesan end-to-end | 210 event/detik |

Angka duplicate rate yang persis 30,0% membuktikan mekanisme dedup menolak seluruh duplikat yang dikirim, dan invarian `received = unique_processed + duplicate_dropped` terpenuhi (20.000 = 14.000 + 6.000).

### 3.2 Distribusi Kerja Antar-Worker

Pembagian beban antar keempat worker terbukti merata, yang menunjukkan paralelisme berjalan efektif:

| Worker | Event diproses |
|---|---|
| worker-1 | 3.537 |
| worker-2 | 3.524 |
| worker-3 | 3.520 |
| worker-4 | 3.544 |

### 3.3 Latency (Jalur Sinkron)

Pengujian latency menggunakan 2.000 permintaan `POST /publish` dengan tingkat konkurensi 50:

| Metrik | Nilai (ms) |
|---|---|
| Rata-rata | 470,88 |
| Persentil 50 | 445,66 |
| Persentil 95 | 764,60 |
| Persentil 99 | 1.280,47 |
| Maksimum | 1.983,59 |

### 3.4 Hasil Uji Konkurensi (Anti Double-Process)

Uji konkurensi inti mendorong satu `event_id` yang sama sebanyak 100 kali ke antrian, lalu memverifikasi bahwa hanya satu baris yang tercatat. Hasilnya konsisten: tepat satu event diproses dan sisanya ditolak sebagai duplikat, membuktikan tidak ada race condition meskipun empat worker berebut kunci yang sama. Pengujian otomatis menggunakan pytest menghasilkan 20 tes lulus pada suite utama dan 1 tes persistensi lulus, mencakup dedup, validasi skema, konsistensi statistik, pemrosesan asinkron, dan persistensi setelah kontainer di-recreate.

---

## 4. Keterkaitan dengan Teori

**Catatan penomoran.** Butir T1–T10 mengikuti pengelompokan tujuan pembelajaran pada lembar soal. Adapun rujukan bab pada sitasi mengacu pada penomoran bab asli buku Coulouris dkk. (2012). Sebagai contoh, materi transaksi dan kontrol konkurensi berada pada Bab 16 buku tersebut, sedangkan publish–subscribe dan message queue dibahas pada Bab 6, serta waktu dan urutan pada Bab 14. Penekanan diberikan pada T8–T9 sesuai ketentuan soal.

### T1 — Karakteristik Sistem Terdistribusi dan Trade-off Desain

Buku acuan menegaskan bahwa pada jaringan komputer, eksekusi program secara konkuren adalah hal yang lazim, dan menetapkan sejumlah tantangan utama, yakni heterogenitas, keterbukaan, keamanan, skalabilitas, penanganan kegagalan, konkurensi, dan transparansi (Coulouris dkk., 2012, Bab 1). Pada aggregator ini, keempat layanan adalah proses terpisah yang hanya saling mengenal melalui antarmuka, sehingga kegagalan satu layanan tidak otomatis menjatuhkan keseluruhan sistem, mencerminkan tantangan penanganan kegagalan parsial. Trade-off desain yang paling menonjol adalah pertukaran antara konsistensi dan ketersediaan. Sistem memilih pengiriman at-least-once demi ketahanan terhadap kehilangan pesan, lalu menebus konsekuensi berupa duplikasi dengan idempotensi di sisi konsumen. Trade-off lain adalah antara kesederhanaan dan jaminan urutan; sistem sengaja tidak menegakkan total ordering karena biayanya tinggi dan tidak diperlukan untuk pencatatan keunikan. Transparansi turut dipertimbangkan melalui penyembunyian lokasi fisik basis data dan broker di balik nama layanan Compose, yang merupakan bentuk transparansi lokasi. Dengan demikian, karakteristik fundamental sistem terdistribusi tercermin langsung pada keputusan arsitektur, dan setiap trade-off dipilih agar selaras dengan tujuan utama, yaitu keandalan pencatatan event tanpa pemrosesan ganda.

### T2 — Kapan Memilih Publish–Subscribe Dibanding Client–Server

Publish–subscribe disebut sebagai teknik komunikasi tak langsung yang paling luas digunakan, di mana publisher menerbitkan event terstruktur ke layanan event dan subscriber menyatakan minat melalui langganan (Coulouris dkk., 2012, Bab 6). Keunggulan utamanya adalah pelepasan keterikatan ruang dan waktu, sehingga produsen dan konsumen tidak harus saling mengenal maupun aktif bersamaan. Dalam sistem ini, publisher cukup mendorong event ke antrian Redis dan worker mengambilnya kapan pun tersedia. Buku acuan menjelaskan message queue sebagai mekanisme bersifat point-to-point, di mana pesan ditaruh ke antrian oleh pengirim lalu diambil oleh satu proses saja; sifat inilah yang dimanfaatkan operasi BRPOP sehingga tiap event hanya diambil oleh satu worker. Sebaliknya, arsitektur client–server lebih cocok untuk interaksi permintaan–jawaban sinkron yang menuntut balasan segera. Alasan teknis pemilihan publish–subscribe pada aggregator adalah kebutuhan menyerap aliran event berfluktuasi dengan banyak konsumen paralel, sekaligus tetap andal ketika konsumen sementara tidak tersedia, karena antrian berperan sebagai peredam beban. Sistem tetap menyediakan jalur client–server melalui `POST /publish` untuk kasus yang membutuhkan kepastian langsung, sehingga kedua gaya dimanfaatkan sesuai konteks penggunaannya.

### T3 — At-least-once vs Exactly-once dan Peran Idempotent Consumer

Buku acuan membahas semantik pemanggilan pada protokol request–reply, di mana pilihan untuk mengirim ulang pesan permintaan menghasilkan jaminan pengiriman yang berbeda, antara lain at-least-once dan at-most-once (Coulouris dkk., 2012, Bab 5). Semantik at-least-once menjamin pesan tersampaikan minimal satu kali namun memungkinkan duplikasi, sedangkan exactly-once sulit dan mahal dicapai pada sistem nyata karena kegagalan jaringan dan kebutuhan koordinasi ketat. Pendekatan praktis yang banyak dipakai adalah memadukan transport at-least-once dengan konsumen idempoten, sehingga efek akhir setara dengan exactly-once meskipun pesan diterima berkali-kali. Pada sistem ini, broker dan publisher beroperasi dengan model at-least-once, sedangkan konsumen menjamin idempotensi melalui batasan unik `(topic, event_id)`. Saat event yang sama tiba berulang, hanya penyisipan pertama yang berhasil sementara sisanya ditolak secara atomik. Hasil uji beban memperlihatkan 6.000 duplikat dari 20.000 event berhasil ditolak seluruhnya, membuktikan konsumen idempoten bekerja sebagaimana diharapkan. Peran konsumen idempoten karenanya sentral, karena ia memindahkan tanggung jawab penjaminan keunikan dari lapisan transport yang rapuh ke lapisan penyimpanan yang memiliki jaminan atomik kuat.

### T4 — Skema Penamaan Topic dan event_id

Layanan penamaan dalam sistem terdistribusi bertugas menjamin identitas unik dan pemetaan yang konsisten terhadap sumber daya (Coulouris dkk., 2012, Bab 13). Sistem ini memakai `topic` sebagai pengelompokan logis aliran event dan `event_id` sebagai pengenal unik tiap event. Untuk menjamin keunikan yang tahan tabrakan, `event_id` dibangkitkan menggunakan UUID versi 4 yang memiliki ruang nilai sangat besar, sehingga probabilitas tabrakan secara praktis dapat diabaikan. Kunci dedup yang sebenarnya adalah pasangan `(topic, event_id)`, bukan `event_id` tunggal, sehingga dua event dengan pengenal sama tetapi topik berbeda diperlakukan sebagai entitas berbeda. Skema ini memberi fleksibilitas karena ruang nama dipartisi per topik, sekaligus mempertahankan keunikan di tingkat pasangan kunci. Implikasinya pada deduplikasi sangat langsung; batasan unik basis data didefinisikan tepat pada pasangan ini, sehingga penamaan dan mekanisme dedup menjadi satu kesatuan yang koheren. Pemilihan UUID juga menghilangkan kebutuhan koordinator pusat untuk membangkitkan pengenal, yang sejalan dengan sifat terdesentralisasi sistem. Dengan demikian, skema penamaan tidak sekadar label, melainkan fondasi yang menentukan kebenaran deduplikasi.

### T5 — Ordering Praktis dan Batasannya

Buku acuan menjelaskan bahwa ketiadaan jam global yang sempurna membuat urutan kejadian didekati dengan jam logis, seperti logical clock yang diperkenalkan Lamport (Coulouris dkk., 2012, Bab 14). Sistem ini menyertakan `timestamp` pada setiap event dan mengandalkan kolom `id` berjenis serial dari basis data sebagai penghitung monoton yang merepresentasikan urutan kedatangan. Pendekatan ini praktis dan murah, tetapi memiliki batasan. Timestamp yang dibuat di sisi publisher dapat tidak sinkron antar sumber sehingga tidak dapat dipercaya untuk menetapkan urutan kausal yang ketat. Penghitung monoton dari basis data hanya mencerminkan urutan masuk ke store, bukan urutan kejadian sebenarnya di dunia nyata. Dampaknya, sistem menerima kemungkinan event tiba out-of-order. Untungnya, tujuan aggregator adalah mencatat keunikan, bukan merekonstruksi urutan kausal, sehingga batasan ini tidak merusak kebenaran. Apabila total ordering dibutuhkan, sistem harus menambah mekanisme seperti logical clock atau vector clock dengan biaya koordinasi yang lebih tinggi. Keputusan untuk tidak menegakkan total ordering merupakan trade-off sadar demi kesederhanaan dan throughput, dan tetap aman karena dedup tidak bergantung pada urutan pemrosesan.

### T6 — Failure Modes dan Mitigasi

Buku acuan menyajikan model kegagalan yang mencakup kegagalan crash pada proses dan kegagalan omission pada komunikasi (Coulouris dkk., 2012, Bab 2). Sistem ini memitigasi kegagalan tersebut melalui beberapa lapisan. Pertama, pengiriman at-least-once dengan pengiriman ulang menjamin event tidak hilang meskipun sebagian percobaan gagal. Kedua, dedup store yang persisten memastikan pengiriman ulang maupun pemrosesan ulang setelah crash tidak menyebabkan pencatatan ganda, karena batasan unik menolak duplikat. Ketiga, worker dirancang agar galat pada satu event tidak menghentikan keseluruhan loop, sehingga kegagalan bersifat terisolasi. Keempat, aggregator menunggu kesiapan basis data saat start melalui retry koneksi, yang menangani kondisi balapan saat seluruh kontainer dijalankan bersamaan oleh Compose. Kelima, persistensi pada named volume menjamin pemulihan setelah kontainer dihapus dan dibuat ulang, sebagaimana dibuktikan pada uji persistensi otomatis. Pemulihan transaksi setelah crash juga sejalan dengan konsep recoverable objects yang dibahas pada Bab 16. Kombinasi retry, store tahan lama, dan pemulihan setelah restart membentuk strategi crash recovery yang sederhana namun efektif, dengan menempatkan jaminan kebenaran pada lapisan penyimpanan yang andal sehingga lapisan transport boleh tidak sempurna tanpa mengorbankan konsistensi akhir.

### T7 — Eventual Consistency dan Peran Idempotency

Buku acuan membahas kriteria kebenaran sistem tereplikasi, dari linearizability yang paling ketat hingga sequential consistency, serta menampilkan contoh layanan yang sengaja melonggarkan konsistensi demi ketersediaan (Coulouris dkk., 2012, Bab 18). Salah satu contohnya, Dynamo milik Amazon, memakai metode optimistik dan tidak memberikan jaminan isolasi penuh seperti ACID, melainkan bentuk konsistensi yang lebih lemah namun cukup untuk aplikasinya. Aggregator ini bersifat eventually consistent pada jalur asinkron; setelah event didorong ke antrian, hasil akhirnya baru tercermin di `GET /events` ketika worker selesai memprosesnya. Selama jeda tersebut pembaca mungkin melihat keadaan yang belum lengkap, namun begitu antrian kosong, keadaan menjadi konsisten dan stabil. Idempotensi dan dedup berperan menjaga agar konvergensi ini benar meskipun ada pengiriman ulang. Tanpa idempotensi, percobaan ulang akan menggandakan data dan membuat keadaan akhir bergantung pada jumlah percobaan, sehingga merusak konvergensi. Dengan idempotensi, berapa pun banyaknya percobaan, keadaan akhir selalu sama, yaitu satu catatan per pasangan kunci. Uji beban memperlihatkan setelah antrian habis, jumlah unik stabil di angka yang diharapkan tanpa pembengkakan akibat duplikat, sehingga properti eventual consistency benar-benar bermakna pada sistem ini.

### T8 — Desain Transaksi: ACID, Isolation, dan Strategi Menghindari Lost-Update

Buku acuan mendefinisikan transaksi atomik melalui akronim ACID, yaitu Atomicity, Consistency, Isolation, dan Durability, dan menjelaskan masalah klasik lost update pada eksekusi konkuren (Coulouris dkk., 2012, Bab 16). Lost update terjadi ketika dua transaksi membaca nilai lama suatu variabel lalu memakainya untuk menghitung nilai baru, sehingga salah satu pembaruan hilang; menurut buku, masalah ini dapat dicegah dengan eksekusi yang serially equivalent. Pada sistem ini, setiap pemrosesan event dibungkus dalam satu transaksi yang mencakup penambahan penghitung statistik dan penyisipan event. Atomicity menjamin kedua operasi berhasil bersama atau gagal bersama, sehingga statistik tidak menyimpang dari isi tabel event. Durability dijamin oleh PostgreSQL yang menulis perubahan ke penyimpanan persisten pada named volume. Sistem menghindari lost update dengan dua cara. Pertama, penghitung dinaikkan memakai pernyataan `UPDATE stats SET kolom = kolom + 1` yang dievaluasi di basis data dan mengambil row-level lock, sehingga pembaruan dari worker berbeda diserialkan dan tidak ada yang hilang. Kedua, pencatatan event memakai `INSERT ... ON CONFLICT DO NOTHING` yang mengandalkan batasan unik, bukan pola baca-lalu-tulis, sehingga tidak ada jendela balapan yang menyebabkan double-write. Tingkat isolasi READ COMMITTED memadai karena kebenaran bergantung pada kunci unik dan kunci baris yang atomik, bukan pada pencegahan phantom read (The PostgreSQL Global Development Group, 2024). Hasilnya, pada uji 100 pengiriman event identik hanya satu yang tercatat, membuktikan tidak ada lost update maupun double-process.

### T9 — Kontrol Konkurensi: Locking, Optimistic, dan Idempotent Write

Buku acuan menyebut tiga metode kontrol konkurensi yang umum, yaitu locking, optimistic concurrency control, dan timestamp ordering, dengan locking sebagai yang paling banyak dipakai pada sistem praktis (Coulouris dkk., 2012, Bab 16). Metode optimistik, yang diusulkan Kung dan Robinson, berangkat dari pengamatan bahwa pada kebanyakan aplikasi kemungkinan dua transaksi mengakses objek yang sama tergolong rendah, sehingga transaksi dibiarkan berjalan seolah tanpa konflik dan konflik baru diselesaikan saat validasi. Sistem ini memadukan teknik tersebut secara komplementer. Pertama, unique constraint pada `(topic, event_id)` bertindak sebagai mekanisme optimistik; alih-alih mengunci di muka, sistem membiarkan penyisipan berjalan dan menyerahkan resolusi konflik kepada basis data, yang menjamin hanya satu penyisipan berhasil. Kedua, idempotent write pattern diwujudkan lewat `INSERT ... ON CONFLICT DO NOTHING`, sehingga penulisan berulang aman tanpa efek samping ganda. Ketiga, row-level lock implisit pada operasi `UPDATE` penghitung menyediakan serialisasi yang diperlukan untuk pembaruan agregat. Kombinasi ini menghindari kebutuhan kunci global yang dikelola aplikasi, yang biasanya menjadi sumber bottleneck dan deadlock. Distribusi kerja yang merata antar worker pada uji beban menunjukkan paralelisme tetap tinggi meskipun ada kontensi, sehingga pendekatan idempotent write berbasis constraint memberi keseimbangan baik antara kebenaran dan kinerja.

### T10 — Orkestrasi, Keamanan Jaringan, Persistensi, dan Observability

Buku acuan membahas virtualisasi pada tingkat sistem operasi sebagai konsep penting dalam sistem terdistribusi, yang menjadi landasan teknologi kontainer (Coulouris dkk., 2012, Bab 7), serta membahas keamanan pada Bab 11, sistem berkas terdistribusi pada Bab 12, dan koordinasi pada Bab 15. Sistem ini diorkestrasi oleh Docker Compose yang mendefinisikan keempat layanan beserta dependensinya, sehingga seluruh sistem dapat dijalankan dengan satu perintah. Dari sisi keamanan jaringan, seluruh komunikasi terjadi di dalam jaringan internal Compose; broker dan basis data tidak mengekspos port keluar, dan hanya aggregator yang membuka port untuk keperluan demo lokal, sehingga permukaan serangan dikurangi. Persistensi ditangani melalui named volume `pg_data` untuk basis data dan `broker_data` untuk Redis, yang menjamin data bertahan meskipun kontainer dihapus, sebagaimana dibuktikan pada uji recreate kontainer. Observability disediakan melalui logging eksplisit pada setiap pemrosesan event yang menandai status baru atau duplikat beserta worker pelaksananya, serta endpoint `GET /stats` yang menampilkan metrik agregat termasuk distribusi kerja antar worker dan kedalaman antrian. Endpoint `GET /healthz` mendukung liveness probe untuk koordinasi kesiapan layanan. Gabungan orkestrasi deklaratif, isolasi jaringan, penyimpanan persisten, dan instrumentasi metrik membentuk sistem yang benar secara fungsional sekaligus dapat dioperasikan dan dipantau.

---

## 5. Daftar Pustaka

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed systems: Concepts and design* (Ed. ke-5). Addison-Wesley.

The PostgreSQL Global Development Group. (2024). *PostgreSQL 16 documentation: Transaction isolation*. https://www.postgresql.org/docs/16/transaction-iso.html
