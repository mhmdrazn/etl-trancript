-- 1. Urutan Nilai IPK Kumulatif (Tertinggi ke Terendah)
SELECT NRP, nama_mahasiswa, ipk_kumulatif FROM Dim_Mahasiswa ORDER BY ipk_kumulatif DESC;

-- 2. Urutan Nilai IPK Kumulatif (Terendah ke Tertinggi)
SELECT NRP, nama_mahasiswa, ipk_kumulatif FROM Dim_Mahasiswa ORDER BY ipk_kumulatif ASC;

-- 3. Urutan Nilai IPK Tahap Sarjana (Tertinggi ke Terendah)
SELECT NRP, nama_mahasiswa, ip_sarjana FROM Dim_Mahasiswa ORDER BY ip_sarjana DESC;

-- 4. Urutan Nilai IPK Tahap Persiapan (Tertinggi ke Terendah)
SELECT NRP, nama_mahasiswa, ip_persiapan FROM Dim_Mahasiswa ORDER BY ip_persiapan DESC;

-- 5. Mahasiswa dengan SKS Lulus Terbanyak
SELECT NRP, nama_mahasiswa, sks_lulus FROM Dim_Mahasiswa ORDER BY sks_lulus DESC;

-- 6. Mahasiswa dengan SKS Lulus Tersedikit
SELECT NRP, nama_mahasiswa, sks_lulus FROM Dim_Mahasiswa ORDER BY sks_lulus ASC;

-- 7. Distribusi Mahasiswa Berdasarkan Status
-- Insight: Memahami komposisi mahasiswa (misal: aktif, cuti, lulus)
SELECT status_mahasiswa, COUNT(id_mahasiswa) AS jumlah_mahasiswa FROM Dim_Mahasiswa GROUP BY status_mahasiswa;

-- 8. Perbandingan IP Tahap Persiapan vs. Sarjana
SELECT NRP, nama_mahasiswa, ip_persiapan, ip_sarjana, (ip_sarjana - ip_persiapan) AS perubahan_ip FROM Dim_Mahasiswa WHERE status_mahasiswa = 'Normal/Aktif' ORDER BY perubahan_ip DESC;

-- 9. Mata Kuliah dengan Nilai Rata-rata Tertinggi/Terendah
SELECT dm.kode_mk, dm.nama_mk, AVG(dn.bobot_nilai) AS rata_rata_bobot_nilai FROM Fact_Transkrip ft JOIN Dim_MataKuliah dm ON ft.id_mk = dm.id_mk JOIN Dim_Nilai dn ON ft.id_nilai = dn.id_nilai GROUP BY dm.kode_mk, dm.nama_mk ORDER BY rata_rata_bobot_nilai DESC;

-- 10. Mata Kuliah Paling banyak Diambil
SELECT dm.kode_mk, dm.nama_mk, COUNT(ft.id_transkrip) AS jumlah_pengambil FROM Fact_Transkrip ft JOIN Dim_MataKuliah dm ON ft.id_mk = dm.id_mk GROUP BY dm.kode_mk, dm.nama_mk ORDER BY jumlah_pengambil DESC;

-- 11. Sebaran Nilai untuk Mata Kuliah Tertentu (Sistem Basis Data)
SELECT dm.nama_mk, dn.huruf_nilai, COUNT(ft.id_transkrip) AS jumlah_mahasiswa FROM Fact_Transkrip ft JOIN Dim_MataKuliah dm ON ft.id_mk = dm.id_mk JOIN Dim_Nilai dn ON ft.id_nilai = dn.id_nilai WHERE dm.nama_mk = 'Sistem Basis Data' GROUP BY dm.nama_mk, dn.huruf_nilai ORDER BY dn.bobot_nilai DESC;

-- 12. Sebaran Nilai untuk Mata Kuliah Tertentu (Administrasi Basis Data)
SELECT dm.nama_mk, dn.huruf_nilai, COUNT(ft.id_transkrip) AS jumlah_mahasiswa FROM Fact_Transkrip ft JOIN Dim_MataKuliah dm ON ft.id_mk = dm.id_mk JOIN Dim_Nilai dn ON ft.id_nilai = dn.id_nilai WHERE dm.nama_mk = 'Administrasi Basis Data' GROUP BY dm.nama_mk, dn.huruf_nilai ORDER BY dn.bobot_nilai DESC;

-- 13. Mata Kuliah dengan Tingkat Kelulusan Tertinggi/Terendah per Semester
SELECT dm.nama_mk, dw.tahun, dw.semester, fk.jml_mahasiswa_lulus AS jumlah_lulus, fk.jml_mahasiswa_tidak_lulus AS jumlah_tidak_lulus, (fk.jml_mahasiswa_lulus * 100.0 / (fk.jml_mahasiswa_lulus + fk.jml_mahasiswa_tidak_lulus)) AS persentase_kelulusan FROM Fact_Kelulusan fk JOIN Dim_MataKuliah dm ON fk.id_mk = dm.id_mk JOIN Dim_Waktu dw ON fk.id_waktu = dw.id_waktu ORDER BY persentase_kelulusan DESC;

-- 14. Tren Kelulusan Mata Kuliah Tertentu dari Waktu ke Waktu (Administrasi Basis Data)
SELECT dw.tahun, dw.semester, (fk.jml_mahasiswa_lulus * 100.0 / (fk.jml_mahasiswa_lulus + fk.jml_mahasiswa_tidak_lulus)) AS persentase_kelulusan FROM Fact_Kelulusan fk JOIN Dim_MataKuliah dm ON fk.id_mk = dm.id_mk JOIN Dim_Waktu dw ON fk.id_waktu = dw.id_waktu WHERE dm.nama_mk = 'Administrasi Basis Data' ORDER BY dw.tahun ASC, 

-- 15. Tren Kelulusan Mata Kuliah Tertentu dari Waktu ke Waktu (Sistem Basis Data)
SELECT dw.tahun, dw.semester, (fk.jml_mahasiswa_lulus * 100.0 / (fk.jml_mahasiswa_lulus + fk.jml_mahasiswa_tidak_lulus)) AS persentase_kelulusan FROM Fact_Kelulusan fk JOIN Dim_MataKuliah dm ON fk.id_mk = dm.id_mk JOIN Dim_Waktu dw ON fk.id_waktu = dw.id_waktu WHERE dm.nama_mk = 'Sistem Basis Data' ORDER BY dw.tahun ASC, dw.semester ASC;

-- 16. Tren IPS Mahasiswa dari Semester ke Semester
SELECT dw.tahun, dw.semester, fps.ips, fps.ipk_saat_itu, fps.perubahan_ips FROM Fact_Prestasi_Semester fps JOIN Dim_Mahasiswa dm ON fps.id_mahasiswa = dm.id_mahasiswa JOIN Dim_Waktu dw ON fps.id_waktu = dw.id_waktu WHERE dm.NRP = '5026231146' ORDER BY dw.tahun ASC, dw.semester ASC;

-- 17. Mahasiswa dengan Kenaikan IPS Terkecil Antar Semester
SELECT dm.NRP, dm.nama_mahasiswa, dw.tahun, dw.semester, fps.perubahan_ips FROM Fact_Prestasi_Semester fps JOIN Dim_Mahasiswa dm ON fps.id_mahasiswa = dm.id_mahasiswa JOIN Dim_Waktu dw ON fps.id_waktu = dw.id_waktu WHERE fps.perubahan_ips IS NOT NULL AND fps.perubahan_ips > 0 ORDER BY fps.perubahan_ips ASC LIMIT 10;

-- 18. Mata Kuliah dengan Rata-rata Bobot Nilai Tertinggi/Terendah (Agregat Akhir)
SELECT dm.kode_mk, dm.nama_mk, fam.rata_rata_bobot_nilai, dw.tahun, dw.semester FROM Fact_Analisis_MataKuliah fam JOIN Dim_MataKuliah dm ON fam.id_mk = dm.id_mk JOIN Dim_Waktu dw ON fam.id_waktu = dw.id_waktu ORDER BY fam.rata_rata_bobot_nilai DESC;

-- 19. Rata  rata IPK Kumulatif tiap semester
SELECT dw.tahun, dw.semester, AVG(dm.ipk_kumulatif) AS rata_rata_ipk_kumulatif FROM Dim_Mahasiswa dm JOIN Fact_Prestasi_Semester fps ON dm.id_mahasiswa = fps.id_mahasiswa JOIN Dim_Waktu dw ON fps.id_waktu = dw.id_waktu GROUP BY dw.tahun, dw.semester ORDER BY dw.tahun ASC, dw.semester ASC;

-- 20. Distribusi Nilai (A, AB, B, dst.) per Mata Kuliah (Administrasi Basis Data)
SELECT dm.nama_mk, fam.jumlah_nilai_A, fam.jumlah_nilai_AB, fam.jumlah_nilai_B, fam.jumlah_nilai_BC, fam.jumlah_nilai_C, fam.jumlah_nilai_D, fam.jumlah_nilai_E, dw.tahun, dw.semester FROM Fact_Analisis_MataKuliah fam JOIN Dim_MataKuliah dm ON fam.id_mk = dm.id_mk JOIN Dim_Waktu dw ON fam.id_waktu = dw.id_waktu WHERE dm.nama_mk = 'Administrasi Basis Data' ORDER BY dw.tahun DESC, dw.semester DESC;

-- 21. Distribusi Nilai (A, AB, B, dst.) per Mata Kuliah (Sistem Basis Data)
SELECT dm.nama_mk, fam.jumlah_nilai_A, fam.jumlah_nilai_AB, fam.jumlah_nilai_B, fam.jumlah_nilai_BC, fam.jumlah_nilai_C, fam.jumlah_nilai_D, fam.jumlah_nilai_E, dw.tahun, dw.semester FROM Fact_Analisis_MataKuliah fam JOIN Dim_MataKuliah dm ON fam.id_mk = dm.id_mk JOIN Dim_Waktu dw ON fam.id_waktu = dw.id_waktu WHERE dm.nama_mk = 'Sistem Basis Data' ORDER BY dw.tahun DESC, dw.semester DESC;

-- 22 . Mahasiswa dengan IPK Kumulatif di atas rata-rata
SELECT NRP, nama_mahasiswa, ipk_kumulatif FROM Dim_Mahasiswa WHERE ipk_kumulatif > (SELECT AVG(ipk_kumulatif) FROM Dim_Mahasiswa);

-- 23 . 10 Mata Kuliah dengan SKS Terbesar
SELECT kode_mk, nama_mk, sks_mk FROM Dim_MataKuliah ORDER BY sks_mk DESC LIMIT 10;

-- 24. Jumlah Mahasiswa yang Mengambil Mata Kuliah "Persiapan" vs. "Sarjana"
SELECT dm.tahap_mk, COUNT(DISTINCT ft.id_mahasiswa) AS jumlah_mahasiswa FROM Fact_Transkrip ft JOIN Dim_MataKuliah dm ON ft.id_mk = dm.id_mk GROUP BY dm.tahap_mk;

-- 25. 5 Mahasiswa dengan IPK Sarjana Terendah (Di antara yang Berstatus Normal/Aktif)
SELECT NRP, nama_mahasiswa, ip_sarjana FROM Dim_Mahasiswa WHERE status_mahasiswa = 'Normal/Aktif' ORDER BY ip_sarjana ASC LIMIT 5;

-- 26 . Semester dengan Rata-rata IPK Tertinggi
SELECT dw.tahun, dw.semester, AVG(fps.ips) AS rata_rata_ips_semester FROM Fact_Prestasi_Semester fps JOIN Dim_Waktu dw ON fps.id_waktu = dw.id_waktu GROUP BY dw.tahun, dw.semester ORDER BY rata_rata_ips_semester DESC;

-- 27 . Mata Kuliah dengan Jumlah Mahasiswa Tidak Lulus Terbanyak per Semester
SELECT dm.nama_mk, dw.tahun, dw.semester, fk.jml_mahasiswa_tidak_lulus FROM Fact_Kelulusan fk JOIN Dim_MataKuliah dm ON fk.id_mk = dm.id_mk JOIN Dim_Waktu dw ON fk.id_waktu = dw.id_waktu ORDER BY fk.jml_mahasiswa_tidak_lulus DESC;

-- 28 . Rata-rata SKS Diambil per Semester oleh Mahasiswa Aktif
SELECT dw.tahun, dw.semester, AVG(fps.sks_diambil_semester) AS rata_rata_sks_diambil FROM Fact_Prestasi_Semester fps JOIN Dim_Waktu dw ON fps.id_waktu = dw.id_waktu JOIN Dim_Mahasiswa dm ON fps.id_mahasiswa = dm.id_mahasiswa WHERE dm.status_mahasiswa = 'Normal/Aktif' GROUP BY dw.tahun, dw.semester ORDER BY dw.tahun ASC, dw.semester ASC;

-- 29 . Jumlah Mahasiswa Baru per Tahun (Berdasarkan NRP unik dan tahun awal tercatat)
SELECT SUBSTRING(NRP, 5, 2) AS tahun_angkatan, COUNT(DISTINCT id_mahasiswa) AS jumlah_mahasiswa FROM Dim_Mahasiswa GROUP BY tahun_angkatan ORDER BY tahun_angkatan ASC;

-- 30 . Mata Kuliah dengan Rentang Bobot Nilai (Min-Max) Terbesar
SELECT dm.nama_mk, MIN(dn.bobot_nilai) AS min_bobot_nilai, MAX(dn.bobot_nilai) AS max_bobot_nilai, (MAX(dn.bobot_nilai) - MIN(dn.bobot_nilai)) AS rentang_bobot_nilai FROM Fact_Transkrip ft JOIN Dim_MataKuliah dm ON ft.id_mk = dm.id_mk JOIN Dim_Nilai dn ON ft.id_nilai = dn.id_nilai GROUP BY dm.nama_mk ORDER BY rentang_bobot_nilai DESC;
