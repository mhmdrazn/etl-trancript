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