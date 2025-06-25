import os
import re
import logging
import pandas as pd
import mysql.connector
from PyPDF2 import PdfReader
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transcript_etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # Update with your password
    'database': 'nilai'
}

class TranscriptETL:
    """Main ETL class for processing academic transcripts"""

    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.connection = None

    def connect_db(self) -> bool:
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            logger.info("Database connection established successfully")
            return True
        except mysql.connector.Error as err:
            logger.error(f"Database connection failed: {err}")
            return False

    ### ONE-TIME HISTORICAL LOAD ###
    # Fungsi ini mempersiapkan struktur database. Tujuannya adalah untuk setup awal.
    def create_warehouse_schema(self):
        """Create the star schema tables based on the new diagram"""
        if not self.connection:
            logger.error("No database connection available")
            return False
            
        cursor = self.connection.cursor()
        
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_config['database']}")
        cursor.execute(f"USE {self.db_config['database']}")
        
        try:
            # Perintah `CREATE TABLE IF NOT EXISTS` memastikan skema hanya dibuat jika belum ada.
            table_sql = [
                """CREATE TABLE IF NOT EXISTS Dim_Mahasiswa (
                    id_mahasiswa INT AUTO_INCREMENT PRIMARY KEY,
                    NRP VARCHAR(20) UNIQUE NOT NULL,
                    nama_mahasiswa VARCHAR(100) NOT NULL,
                    status_mahasiswa VARCHAR(50),
                    ipk_kumulatif DECIMAL(3,2),
                    sks_tempuh INT,
                    sks_lulus INT,
                    ip_persiapan DECIMAL(3,2),
                    sks_persiapan INT,
                    ip_sarjana DECIMAL(3,2),
                    sks_sarjana INT
                )""",
                
                """CREATE TABLE IF NOT EXISTS Dim_MataKuliah (
                    id_mk INT AUTO_INCREMENT PRIMARY KEY,
                    kode_mk VARCHAR(20) UNIQUE NOT NULL,
                    nama_mk VARCHAR(200) NOT NULL,
                    sks_mk INT NOT NULL,
                    tahap_mk VARCHAR(50) NOT NULL
                )""",

                """CREATE TABLE IF NOT EXISTS Dim_Waktu (
                    id_waktu INT AUTO_INCREMENT PRIMARY KEY,
                    tahun INT NOT NULL,
                    semester VARCHAR(20) NOT NULL,
                    UNIQUE KEY unique_time (tahun, semester)
                )""",
                
                """CREATE TABLE IF NOT EXISTS Dim_Nilai (
                    id_nilai INT AUTO_INCREMENT PRIMARY KEY,
                    huruf_nilai VARCHAR(5) UNIQUE NOT NULL,
                    bobot_nilai DECIMAL(3,2) NOT NULL
                )""",
                                
                """CREATE TABLE IF NOT EXISTS Fact_Transkrip (
                    id_transkrip INT AUTO_INCREMENT PRIMARY KEY,
                    id_mahasiswa INT NOT NULL,
                    id_mk INT NOT NULL,
                    id_waktu INT NOT NULL,
                    id_nilai INT NOT NULL,
                    bobot_matkul DECIMAL(4,2) NOT NULL,
                    FOREIGN KEY (id_mahasiswa) REFERENCES Dim_Mahasiswa(id_mahasiswa),
                    FOREIGN KEY (id_mk) REFERENCES Dim_MataKuliah(id_mk),
                    FOREIGN KEY (id_waktu) REFERENCES Dim_Waktu(id_waktu),
                    FOREIGN KEY (id_nilai) REFERENCES Dim_Nilai(id_nilai),
                    UNIQUE KEY unique_transcript (id_mahasiswa, id_mk, id_waktu)
                )""",
                
                """CREATE TABLE IF NOT EXISTS Fact_Kelulusan (
                    id_kelulusan INT AUTO_INCREMENT PRIMARY KEY,
                    id_mk INT NOT NULL,
                    id_waktu INT NOT NULL,
                    jml_mahasiswa_lulus INT DEFAULT 0,
                    jml_mahasiswa_tidak_lulus INT DEFAULT 0,
                    FOREIGN KEY (id_mk) REFERENCES Dim_MataKuliah(id_mk),
                    FOREIGN KEY (id_waktu) REFERENCES Dim_Waktu(id_waktu),
                    UNIQUE KEY unique_kelulusan (id_mk, id_waktu)
                )""",

                """CREATE TABLE IF NOT EXISTS Fact_Prestasi_Semester (
                    id_prestasi_semester INT AUTO_INCREMENT PRIMARY KEY,
                    id_mahasiswa INT NOT NULL,
                    id_waktu INT NOT NULL,
                    ips DECIMAL(3,2) NOT NULL,
                    sks_diambil_semester INT NOT NULL,
                    sks_lulus_semester INT NOT NULL,
                    jumlah_mk_semester INT NOT NULL,
                    ipk_saat_itu DECIMAL(3,2) NOT NULL,
                    perubahan_ips DECIMAL(4,2),
                    FOREIGN KEY (id_mahasiswa) REFERENCES Dim_Mahasiswa(id_mahasiswa),
                    FOREIGN KEY (id_waktu) REFERENCES Dim_Waktu(id_waktu),
                    UNIQUE KEY unique_prestasi (id_mahasiswa, id_waktu)
                )""",

                """CREATE TABLE IF NOT EXISTS Fact_Analisis_MataKuliah (
                    id_analisis_mk INT AUTO_INCREMENT PRIMARY KEY,
                    id_mk INT NOT NULL,
                    id_waktu INT NOT NULL,
                    rata_rata_bobot_nilai DECIMAL(3,2),
                    persentase_kelulusan DECIMAL(5,2),
                    jumlah_pengambil_mk INT,
                    jumlah_nilai_A INT DEFAULT 0,
                    jumlah_nilai_AB INT DEFAULT 0,
                    jumlah_nilai_B INT DEFAULT 0,
                    jumlah_nilai_BC INT DEFAULT 0,
                    jumlah_nilai_C INT DEFAULT 0,
                    jumlah_nilai_D INT DEFAULT 0,
                    jumlah_nilai_E INT DEFAULT 0,
                    FOREIGN KEY (id_mk) REFERENCES Dim_MataKuliah(id_mk),
                    FOREIGN KEY (id_waktu) REFERENCES Dim_Waktu(id_waktu),
                    UNIQUE KEY unique_analisis_mk (id_mk, id_waktu)
                )"""
            ]
            
            for sql in table_sql:
                cursor.execute(sql)
            
            self.connection.commit()
            
            self._insert_reference_data()
            
        except mysql.connector.Error as err:
            logger.error(f"Error creating schema: {err}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()
            
        return True
    
    ### ONE-TIME HISTORICAL LOAD ###
    # Mengisi tabel `Dim_Nilai` dengan data referensi yang statis.
    def _insert_reference_data(self):
        """Insert reference data for grades"""
        cursor = self.connection.cursor()
        
        try:
            grades = [('A', 4.0), ('AB', 3.5), ('B', 3.0), ('BC', 2.5), ('C', 2.0), ('D', 1.0), ('E', 0.0)]
            # Logika `if...==0` ini memastikan data referensi hanya di-INSERT sekali saat tabel kosong.
            cursor.execute("SELECT COUNT(*) FROM Dim_Nilai")
            if cursor.fetchone()[0] == 0:
                cursor.executemany("INSERT INTO Dim_Nilai (huruf_nilai, bobot_nilai) VALUES (%s, %s)", grades)
            
            self.connection.commit()
            
        except mysql.connector.Error as err:
            logger.error(f"Error inserting reference data: {err}")
            self.connection.rollback()
        finally:
            cursor.close()
    
    def extract_pdf_text(self, pdf_path: str) -> str:
        # Extraxtion logic (no changes)
        try:
            with open(pdf_path, 'rb') as file:
                reader = PdfReader(file)
                full_text = ""
                for page in reader.pages:
                    page_text = page.extract_text() or ""
                    cleaned_text = re.sub(r'([a-zA-Z])\s([a-z])', r'\1\2', page_text)
                    conjunctions = ['dan', 'atau', 'serta', 'untuk', 'ke', 'dari', 'pada']
                    for word in conjunctions:
                        pattern = re.compile(f'([a-z])({word}\\b)', re.IGNORECASE)
                        cleaned_text = pattern.sub(r'\1 \2', cleaned_text)
                    cleaned_text = re.sub(r"([a-z])([A-Z])", r"\1 \2", cleaned_text)
                    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                    full_text += cleaned_text + "\n"
                return full_text
        except Exception as e:
            logger.error(f"Error reading PDF {pdf_path}: {e}")
            return ""
    
    def parse_transcript(self, text: str) -> Optional[Dict]:
        # Parsing logic (no changes)
        try:
            data = {'student': {}, 'courses': []}
            student_info = self._parse_student_info(text)
            if not student_info: return None
            data['student'] = student_info
            courses = self._parse_courses(text)
            if not courses: return None
            data['courses'] = courses
            return data
        except Exception as e:
            logger.error(f"Error parsing transcript: {e}")
            return None
    
    def _parse_student_info(self, text: str) -> Optional[Dict]:
        # Parsing logic (no changes)
        try:
            nrp_nama_match = re.search(r'NRP\s*/\s*Nama\s*(\d+)\s*/\s*(.*?)\s*SKS Tempuh', text, re.DOTALL)
            sks_match = re.search(r'SKS\s*Tempuh\s*/\s*SKS\s*Lulus\s*(\d+)\s*/\s*(\d+)', text)
            status_match = re.search(r'Status\s*(.*?)(?=\s*Tahap|---)', text, re.DOTALL)
            ipk_match = re.search(r'IPK\s*([\d.]+)', text)
            ip_persiapan_match = re.search(r'IP Tahap Persiapan\s*:\s*([\d.]+)', text, re.IGNORECASE)
            sks_persiapan_match = re.search(r'Total Sks Tahap Persiapan\s*:\s*(\d+)', text, re.IGNORECASE)
            ip_sarjana_match = re.search(r'IP Tahap Sarjana\s*:\s*([\d.]+)', text, re.IGNORECASE)
            sks_sarjana_match = re.search(r'Total Sks Tahap Sarjana\s*:\s*(\d+)', text, re.IGNORECASE)
            if not all([nrp_nama_match, sks_match, status_match, ipk_match]): return None
            nama = re.sub(r'\s+', ' ', nrp_nama_match.group(2)).strip()
            status = re.sub(r'\s+', ' ', status_match.group(1)).strip()
            return {'nrp': nrp_nama_match.group(1).strip(), 'nama_mahasiswa': nama, 'status_mahasiswa': status, 'sks_tempuh': int(sks_match.group(1)), 'sks_lulus': int(sks_match.group(2)), 'ipk': float(ipk_match.group(1)), 'ip_persiapan': float(ip_persiapan_match.group(1)) if ip_persiapan_match else 0.0, 'sks_persiapan': int(sks_persiapan_match.group(1)) if sks_persiapan_match else 0, 'ip_sarjana': float(ip_sarjana_match.group(1)) if ip_sarjana_match else 0.0, 'sks_sarjana': int(sks_sarjana_match.group(1)) if sks_sarjana_match else 0}
        except Exception as e:
            logger.error(f"Error parsing student info: {e}")
            return None
    
    def _parse_courses(self, text: str) -> List[Dict]:
        # Parsing logic (no changes)
        try:
            courses = []
            course_pattern = r'([A-Z]{2}\d{5,6})\s*(.*?)\s*(\d)\s*(\d{4}/(?:Gs|Gn)/[A-Z]{1,2})\s*([A-Z]{1,2})'
            matches = re.finditer(course_pattern, text, re.DOTALL)
            sarjana_start_match = re.search(r'Tahap:\s*Sarjana', text)
            sarjana_start_pos = sarjana_start_match.start() if sarjana_start_match else -1
            for match in matches:
                course_name = re.sub(r'\s+', ' ', match.group(2)).strip()
                sks = int(match.group(3))
                hist_info = match.group(4)
                grade = match.group(5)
                year_sem_match = re.search(r'(\d{4})/(Gs|Gn)', hist_info)
                if not year_sem_match: continue
                year, sem_code = year_sem_match.groups()
                phase = 'Sarjana' if sarjana_start_pos != -1 and match.start() > sarjana_start_pos else 'Persiapan'
                semester = 'Gasal' if sem_code == 'Gs' else 'Genap'
                courses.append({'kode_mk': match.group(1).strip(), 'nama_mk': course_name, 'sks_mk': sks, 'tahun': int(year), 'semester': semester, 'huruf_nilai': grade.strip(), 'tahap_mk': phase})
            return courses
        except Exception as e:
            logger.error(f"Error parsing courses: {e}")
            return []
    
    def load_to_warehouse(self, data: Dict) -> bool:
        """Load parsed data into the new data warehouse schema"""
        if not self.connection:
            logger.error("No database connection available")
            return False
        
        cursor = self.connection.cursor(dictionary=True)
        
        try:
            id_mahasiswa = self._load_mahasiswa(cursor, data['student'])
            if not id_mahasiswa:
                return False
            
            for course in data['courses']:
                self._load_course_fact(cursor, id_mahasiswa, course)
            
            self._update_prestasi_semester(cursor, id_mahasiswa, data['courses'])

            self.connection.commit()
            return True
        except mysql.connector.Error as err:
            logger.error(f"Database error during load: {err}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()
    
    ### INCREMENTAL LOAD ###
    # Menerapkan logika "Update or Insert" (UPSERT) untuk data mahasiswa.
    def _load_mahasiswa(self, cursor, student_data: Dict) -> Optional[int]:
        """Load or update student data and return student key"""
        try:
            cursor.execute("SELECT id_mahasiswa FROM Dim_Mahasiswa WHERE NRP = %s", (student_data['nrp'],))
            result = cursor.fetchone()
            
            student_columns = (
                student_data['nama_mahasiswa'], student_data['status_mahasiswa'],
                student_data['ipk'], student_data['sks_tempuh'], student_data['sks_lulus'],
                student_data['ip_persiapan'], student_data['sks_persiapan'],
                student_data['ip_sarjana'], student_data['sks_sarjana']
            )

            # Jika mahasiswa sudah ada, UPDATE datanya. Jika tidak, INSERT data baru.
            if result:
                id_mahasiswa = result['id_mahasiswa']
                update_sql = """UPDATE Dim_Mahasiswa SET nama_mahasiswa = %s, status_mahasiswa = %s, ipk_kumulatif = %s, sks_tempuh = %s, sks_lulus = %s, ip_persiapan = %s, sks_persiapan = %s, ip_sarjana = %s, sks_sarjana = %s WHERE id_mahasiswa = %s"""
                cursor.execute(update_sql, student_columns + (id_mahasiswa,))
            else:
                insert_sql = """INSERT INTO Dim_Mahasiswa (NRP, nama_mahasiswa, status_mahasiswa, ipk_kumulatif, sks_tempuh, sks_lulus, ip_persiapan, sks_persiapan, ip_sarjana, sks_sarjana) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(insert_sql, (student_data['nrp'],) + student_columns)
                id_mahasiswa = cursor.lastrowid
            
            return id_mahasiswa
        except mysql.connector.Error as err:
            logger.error(f"Error loading student data: {err}")
            return None
    
    ### INCREMENTAL LOAD ###
    # Memuat fakta transkrip sambil memastikan tidak ada duplikasi dan memperbarui agregat.
    def _load_course_fact(self, cursor, id_mahasiswa: int, course_data: Dict) -> bool:
        """Load course, time, grade dimensions and the transcript fact"""
        try:
            id_mk = self._get_or_create_key(cursor, "Dim_MataKuliah", "id_mk", "kode_mk", course_data['kode_mk'], 
                                             "INSERT INTO Dim_MataKuliah (kode_mk, nama_mk, sks_mk, tahap_mk) VALUES (%s, %s, %s, %s)",
                                             (course_data['kode_mk'], course_data['nama_mk'], course_data['sks_mk'], course_data['tahap_mk']))
            
            id_waktu = self._get_or_create_key(cursor, "Dim_Waktu", "id_waktu", "tahun = %s AND semester = %s", (course_data['tahun'], course_data['semester']),
                                              "INSERT INTO Dim_Waktu (tahun, semester) VALUES (%s, %s)",
                                              (course_data['tahun'], course_data['semester']))

            cursor.execute("SELECT id_nilai, bobot_nilai FROM Dim_Nilai WHERE huruf_nilai = %s", (course_data['huruf_nilai'],))
            nilai_result = cursor.fetchone()
            if not nilai_result: return False
            id_nilai, bobot_nilai = nilai_result['id_nilai'], nilai_result['bobot_nilai']
            bobot_matkul = course_data['sks_mk'] * bobot_nilai

            # Pengecekan manual untuk mencegah duplikasi di `Fact_Transkrip`.
            cursor.execute("SELECT id_transkrip FROM Fact_Transkrip WHERE id_mahasiswa = %s AND id_mk = %s AND id_waktu = %s", (id_mahasiswa, id_mk, id_waktu))
            if not cursor.fetchone():
                cursor.execute("INSERT INTO Fact_Transkrip (id_mahasiswa, id_mk, id_waktu, id_nilai, bobot_matkul) VALUES (%s, %s, %s, %s, %s)", (id_mahasiswa, id_mk, id_waktu, id_nilai, bobot_matkul))
            
            # `ON DUPLICATE KEY UPDATE` untuk agregasi data kelulusan secara inkremental.
            lulus = course_data['huruf_nilai'] not in ['D', 'E']
            if lulus:
                upsert_sql = """INSERT INTO Fact_Kelulusan (id_mk, id_waktu, jml_mahasiswa_lulus, jml_mahasiswa_tidak_lulus) VALUES (%s, %s, 1, 0) ON DUPLICATE KEY UPDATE jml_mahasiswa_lulus = jml_mahasiswa_lulus + 1"""
            else:
                upsert_sql = """INSERT INTO Fact_Kelulusan (id_mk, id_waktu, jml_mahasiswa_lulus, jml_mahasiswa_tidak_lulus) VALUES (%s, %s, 0, 1) ON DUPLICATE KEY UPDATE jml_mahasiswa_tidak_lulus = jml_mahasiswa_tidak_lulus + 1"""
            cursor.execute(upsert_sql, (id_mk, id_waktu))
            
            return True
        except (mysql.connector.Error, TypeError) as err:
            logger.error(f"Error loading course fact for '{course_data['kode_mk']}': {err}")
            return False

    ### INCREMENTAL LOAD ###
    # Menghitung dan memperbarui snapshot prestasi mahasiswa per semester.
    def _update_prestasi_semester(self, cursor, id_mahasiswa: int, all_courses: List[Dict]):
        """Calculate and load/update periodic snapshot data for each semester."""
        cursor.execute("SELECT huruf_nilai, bobot_nilai FROM Dim_Nilai")
        grade_weights = {row['huruf_nilai']: row['bobot_nilai'] for row in cursor.fetchall()}
        
        courses_by_semester = defaultdict(list)
        for course in all_courses:
            courses_by_semester[(course['tahun'], course['semester'])].append(course)

        total_sks_kumulatif, total_poin_kumulatif, ips_sebelumnya = 0, 0, 0.0

        for (tahun, semester), courses_in_sem in sorted(courses_by_semester.items()):
            sks_diambil_semester = sum(c['sks_mk'] for c in courses_in_sem)
            poin_semester = sum(c['sks_mk'] * grade_weights.get(c['huruf_nilai'], 0) for c in courses_in_sem)
            ips = (poin_semester / sks_diambil_semester) if sks_diambil_semester > 0 else 0.0
            total_sks_kumulatif += sks_diambil_semester
            total_poin_kumulatif += poin_semester
            ipk_saat_itu = (total_poin_kumulatif / total_sks_kumulatif) if total_sks_kumulatif > 0 else 0.0
            id_waktu = self._get_or_create_key(cursor, "Dim_Waktu", "id_waktu", "tahun = %s AND semester = %s", (tahun, semester), "INSERT INTO Dim_Waktu (tahun, semester) VALUES (%s, %s)", (tahun, semester))
            
            # UPSERT: Memasukkan data prestasi semester baru atau memperbarui yang sudah ada.
            upsert_sql = """INSERT INTO Fact_Prestasi_Semester (id_mahasiswa, id_waktu, ips, sks_diambil_semester, sks_lulus_semester, jumlah_mk_semester, ipk_saat_itu, perubahan_ips) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE ips = VALUES(ips), sks_diambil_semester = VALUES(sks_diambil_semester), sks_lulus_semester = VALUES(sks_lulus_semester), jumlah_mk_semester = VALUES(jumlah_mk_semester), ipk_saat_itu = VALUES(ipk_saat_itu), perubahan_ips = VALUES(perubahan_ips)"""
            sks_lulus_semester = sum(c['sks_mk'] for c in courses_in_sem if c['huruf_nilai'] not in ['D', 'E'])
            perubahan_ips = ips - ips_sebelumnya if ips_sebelumnya > 0 else 0.0
            cursor.execute(upsert_sql, (id_mahasiswa, id_waktu, ips, sks_diambil_semester, sks_lulus_semester, len(courses_in_sem), ipk_saat_itu, perubahan_ips))
            
            ips_sebelumnya = ips

    ### INCREMENTAL LOAD ###
    # Fungsi pembantu untuk mencari ID dari sebuah entitas di tabel dimensi.
    # Jika tidak ada, entitas baru akan dibuat. Ini mencegah duplikasi data master.
    def _get_or_create_key(self, cursor, table, key_col, where_col, where_val, insert_sql, insert_val) -> Optional[int]:
        """Generic function to get or create a dimension key."""
        try:
            query = f"SELECT {key_col} FROM {table} WHERE {where_col if isinstance(where_val, tuple) else where_col + ' = %s'}"
            cursor.execute(query, where_val if isinstance(where_val, tuple) else (where_val,))
            result = cursor.fetchone()
            
            if result:
                return result[key_col]
            else:
                cursor.execute(insert_sql, insert_val)
                return cursor.lastrowid
        except mysql.connector.Error as err:
            logger.error(f"Error with dimension {table}: {err}")
            return None

    def process_folder(self, folder_path: str) -> Dict[str, int]:
        """Process all PDF files in a folder"""
        if not os.path.isdir(folder_path):
            logger.error(f"Folder not found: {folder_path}")
            return {'processed': 0, 'failed': 0}
        
        stats = {'processed': 0, 'failed': 0}
        pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]
        logger.info(f"Found {len(pdf_files)} PDF files to process")
        
        # Loop ini berlaku sebagai historical load (saat pertama kali) dan
        # incremental load (saat dijalankan kembali dengan file baru).
        for filename in pdf_files:
            pdf_path = os.path.join(folder_path, filename)
            logger.info(f"Processing: {filename}")
            try:
                text = self.extract_pdf_text(pdf_path)
                if text and (data := self.parse_transcript(text)) and self.load_to_warehouse(data):
                    stats['processed'] += 1
                else:
                    stats['failed'] += 1
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                stats['failed'] += 1
        
        return stats
    
    ### INCREMENTAL LOAD ###
    # Mengagregasi data dari `Fact_Transkrip` ke tabel analisis.
    def populate_analisis_matakuliah(self):
        """Populate the Fact_Analisis_MataKuliah table by aggregating data from other tables."""
        if not self.connection:
            logger.error("No database connection available for final aggregation.")
            return

        logger.info("Starting final aggregation for Fact_Analisis_MataKuliah...")
        cursor = self.connection.cursor(dictionary=True)
        try:
            query = """SELECT ft.id_mk, ft.id_waktu, COUNT(ft.id_transkrip) AS jumlah_pengambil_mk, SUM(CASE WHEN dn.huruf_nilai NOT IN ('D', 'E') THEN 1 ELSE 0 END) AS jml_lulus, AVG(dn.bobot_nilai) AS rata_rata_bobot_nilai, SUM(CASE WHEN dn.huruf_nilai = 'A' THEN 1 ELSE 0 END) AS jumlah_nilai_A, SUM(CASE WHEN dn.huruf_nilai = 'AB' THEN 1 ELSE 0 END) AS jumlah_nilai_AB, SUM(CASE WHEN dn.huruf_nilai = 'B' THEN 1 ELSE 0 END) AS jumlah_nilai_B, SUM(CASE WHEN dn.huruf_nilai = 'BC' THEN 1 ELSE 0 END) AS jumlah_nilai_BC, SUM(CASE WHEN dn.huruf_nilai = 'C' THEN 1 ELSE 0 END) AS jumlah_nilai_C, SUM(CASE WHEN dn.huruf_nilai = 'D' THEN 1 ELSE 0 END) AS jumlah_nilai_D, SUM(CASE WHEN dn.huruf_nilai = 'E' THEN 1 ELSE 0 END) AS jumlah_nilai_E FROM Fact_Transkrip ft JOIN Dim_Nilai dn ON ft.id_nilai = dn.id_nilai GROUP BY ft.id_mk, ft.id_waktu;"""
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                persentase_kelulusan = (row['jml_lulus'] / row['jumlah_pengambil_mk'] * 100) if row['jumlah_pengambil_mk'] > 0 else 0
                
                # UPSERT: Mengisi atau memperbarui tabel agregat dengan hasil analisis terbaru.
                upsert_sql = """INSERT INTO Fact_Analisis_MataKuliah (id_mk, id_waktu, rata_rata_bobot_nilai, persentase_kelulusan, jumlah_pengambil_mk, jumlah_nilai_A, jumlah_nilai_AB, jumlah_nilai_B, jumlah_nilai_BC, jumlah_nilai_C, jumlah_nilai_D, jumlah_nilai_E) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE rata_rata_bobot_nilai = VALUES(rata_rata_bobot_nilai), persentase_kelulusan = VALUES(persentase_kelulusan), jumlah_pengambil_mk = VALUES(jumlah_pengambil_mk), jumlah_nilai_A = VALUES(jumlah_nilai_A), jumlah_nilai_AB = VALUES(jumlah_nilai_AB), jumlah_nilai_B = VALUES(jumlah_nilai_B), jumlah_nilai_BC = VALUES(jumlah_nilai_BC), jumlah_nilai_C = VALUES(jumlah_nilai_C), jumlah_nilai_D = VALUES(jumlah_nilai_D), jumlah_nilai_E = VALUES(jumlah_nilai_E)"""
                cursor.execute(upsert_sql, (row['id_mk'], row['id_waktu'], row['rata_rata_bobot_nilai'], persentase_kelulusan, row['jumlah_pengambil_mk'], row['jumlah_nilai_A'], row['jumlah_nilai_AB'], row['jumlah_nilai_B'], row['jumlah_nilai_BC'], row['jumlah_nilai_C'], row['jumlah_nilai_D'], row['jumlah_nilai_E']))

            self.connection.commit()
            logger.info(f"Successfully populated/updated Fact_Analisis_MataKuliah for {len(results)} course-semester combinations.")
        except mysql.connector.Error as err:
            logger.error(f"Error during final aggregation: {err}")
            self.connection.rollback()
        finally:
            cursor.close()

    def close_connection(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")

def main():
    etl = TranscriptETL(DB_CONFIG)
    
    try:
        if not etl.connect_db():
            return
        
        # Langkah 1: Setup skema awal (one-time).
        if not etl.create_warehouse_schema():
            return
        
        folder_path = 'source/'
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            logger.info(f"Input folder '{folder_path}' created. Please add PDF files to this folder.")
            return

        # Langkah 2: Proses semua file (historical/incremental load).
        logger.info("Starting transcript processing...")
        stats = etl.process_folder(folder_path)
        logger.info(f"Processing Complete. Processed: {stats['processed']}, Failed: {stats['failed']}")
        
        # Langkah 3: Lakukan agregasi akhir (incremental update).
        etl.populate_analisis_matakuliah()

    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
    finally:
        etl.close_connection()

if __name__ == "__main__":
    main()