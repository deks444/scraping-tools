import os
import sys
import time
import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Force UTF-8 encoding for stdout/stderr to avoid charmap errors on Windows
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass
        
# --- KONFIGURASI ---
# --- KONFIGURASI ---
from dotenv import load_dotenv

load_dotenv()
# --- KONFIGURASI ---
script_dir = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE = os.path.join(script_dir, "download_history.xlsx")
BASE_DOWNLOAD_API = "https://streamapi.web.id/api-dramabox/drama.php"
BASE_HOME_API = "https://streamapi.web.id/api-dramabox/index.php"
MAX_WORKERS = 5 # Jumlah thread download bersamaan (Parallel)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json'
}

# Auto-load API Key if exists
api_key = os.getenv("STREAMAPI_KEY") or os.getenv("API_KEY")
if api_key:
    HEADERS['X-API-Key'] = api_key

# --- UTILITIES ---

def format_size(size_bytes):
    """Mengubah ukuran bytes menjadi format yang mudah dibaca (MB, GB)."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(0)
    p = 1024
    s = size_bytes
    while s >= p and i < len(size_name) - 1:
        s /= p
        i += 1
    return f"{s:.2f} {size_name[i]}"

def get_with_retry(url, retries=3, delay=1, timeout=30):
    """Helper function untuk request dengan retry logic."""
    for i in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout, verify=False)
            return response
        except requests.exceptions.RequestException:
            time.sleep(delay * (i + 1)) 
    raise Exception("Max retries exceeded")

def get_head_with_retry(url, retries=3, timeout=10):
    """Helper untuk HEAD request yang lebih robust."""
    for i in range(retries):
        try:
            return requests.head(url, headers=HEADERS, allow_redirects=True, timeout=timeout, verify=False)
        except:
             time.sleep(1)
    return None

def get_file_size(url):
    """Mendapatkan ukuran file dari header HTTP."""
    try:
        response = get_head_with_retry(url, timeout=10)
        if response is None or response.status_code != 200:
             # Fallback ke GET
             try:
                response = requests.get(url, headers=HEADERS, stream=True, allow_redirects=True, timeout=15, verify=False)
                val = int(response.headers.get('content-length', 0))
                response.close()
                return val
             except:
                return 0
        
        if response:
            return int(response.headers.get('content-length', 0))
    except (requests.exceptions.RequestException, ValueError):
        return 0
    return 0

def download_file(url, filename, folder):
    """
    Generic download function for video and images.
    """
    path = os.path.join(folder, filename)
    
    # Resume check sederhana
    remote_size = get_file_size(url)
    if os.path.exists(path):
        local_size = os.path.getsize(path)
        if local_size > 0:
            if remote_size == 0 or local_size == remote_size:
                return True, f"{filename} (Skipped)"

    try:
        response = requests.get(url, headers=HEADERS, stream=True, timeout=60, verify=False)
        response.raise_for_status()
        
        block_size = 8192 
        with open(path, 'wb') as file:
            for data in response.iter_content(block_size):
                file.write(data)
                
        return True, filename
    except Exception as e:
        return False, f"{filename} Error: {e}"

# --- EXCEL HISTORY MANAGER ---

def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            return pd.read_excel(HISTORY_FILE)
        except Exception:
            return pd.DataFrame(columns=["BookID", "Title", "TotalChapters", "DownloadDate", "Status"])
    return pd.DataFrame(columns=["BookID", "Title", "TotalChapters", "DownloadDate", "Status"])

def save_to_history(book_id, title, total_chapters, status="Completed"):
    df = load_history()
    new_data = {
        "BookID": str(book_id),
        "Title": title,
        "TotalChapters": total_chapters,
        "DownloadDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Status": status
    }
    df['BookID'] = df['BookID'].astype(str)
    df = df[df["BookID"] != str(book_id)]
    df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)
    try:
        df.to_excel(HISTORY_FILE, index=False)
    except: pass

def check_is_downloaded(book_id):
    df = load_history()
    if df.empty: return None
    df['BookID'] = df['BookID'].astype(str)
    result = df[df["BookID"] == str(book_id)]
    if not result.empty: return result.iloc[0]
    return None

# --- CORE LOGIC ---

def process_book_parallel(book_data):
    """
    Download satu buku (Video & Cover) & Metadata Excel.
    Path: ../{bookId} (diluar folder script ini)
    Filenames: {bookId}_{chapterId}.mp4 / .jpg
    """
    book_id_arg = str(book_data['id'])
    
    # 1. Fetch Detail
    print(f"\n   >> Mengambil detail buku ID: {book_id_arg} ...")
    url = f"{BASE_DOWNLOAD_API}?bookId={book_id_arg}"
    
    try:
        response = get_with_retry(url, timeout=20)
        json_resp = response.json()
    except Exception as e:
        print(f"   [Error] Gagal fetch API: {e}")
        return False

    if not json_resp.get("success") and json_resp.get("status") is not True:
         if not json_resp.get("data"):
            print(f"   [Error] API response not success. Msg: {json_resp.get('message')}")
            return False

    data_content = json_resp.get("data", {})
    
    # Update parsing logic for new API structure
    book_info = data_content.get("dramaInfo", {})
    chapters = data_content.get("chapters", [])
    
    # Fallback legacy check
    if not book_info and "book" in data_content:
         book_info = data_content.get("book", {})
    if not chapters and "chapterList" in data_content:
         chapters = data_content.get("chapterList", [])

    if not book_info:
        # Last resort, pakai data dari parameter
        book_info = {"bookId": book_id_arg, "bookName": book_data.get("name", "Unknown")}

    real_book_id = str(book_info.get("bookId", book_info.get("id", book_id_arg)))
    book_name = book_info.get("bookName", book_info.get("title", "Unknown"))
    total_chapters = len(chapters)

    # 2. Folder Setup (Relative to Script Directory)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    target_folder = os.path.join(current_dir, real_book_id)
    
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
        
    print(f"   >> Target Folder: {target_folder}")
    print(f"   >> Judul: {book_name} | Total: {total_chapters} Eps")

    # 3. Save Description to Excel
    try:
        info_flat = book_info.copy()
        for k, v in info_flat.items():
            if isinstance(v, list):
                 info_flat[k] = str(v)
            elif isinstance(v, dict):
                 info_flat[k] = str(v)
        
        desc_path = os.path.join(target_folder, f"deskripsi_{real_book_id}.xlsx")
        pd.DataFrame([info_flat]).to_excel(desc_path, index=False)
        print(f"   >> Deskripsi tersimpan: {os.path.basename(desc_path)}")
    except Exception as e:
        print(f"   [Warning] Gagal simpan excel deskripsi: {e}")

    # 4. Prepare Download Tasks
    tasks = []
    for chap in chapters:
        c_id = str(chap.get("id"))
        
        # Task Video
        vid_url = chap.get("mp4") or chap.get("url") # Fallback key
        if vid_url:
            tasks.append({
                "url": vid_url,
                "file": f"{real_book_id}_{c_id}.mp4"
            })
            
        # Task Cover
        cover_url = chap.get("cover") or chap.get("thumbnail")
        if cover_url:
             tasks.append({
                "url": cover_url,
                "file": f"{real_book_id}_{c_id}.jpg"
            })

    # 5. Execute Download
    success_count = 0
    total_tasks = len(tasks)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(download_file, t["url"], t["file"], target_folder): t 
            for t in tasks
        }
        
        with tqdm(total=total_tasks, unit="file", desc="      Download") as pbar:
            for future in as_completed(future_to_task):
                status, msg = future.result()
                if status:
                    success_count += 1
                pbar.update(1)

    # 6. Save History Internal
    status_str = "Completed" if success_count >= total_tasks else "Partial"
    save_to_history(real_book_id, book_name, total_chapters, status_str)
        
    print(f"      Selesai. {success_count}/{total_tasks} files berhasil.")
    return True


def fetch_all_available_books():
    """Scan semua buku dari API baru dengan Pagination."""
    all_books = []
    seen_ids = set()
    page = 1
    
    print("\nSedang memindai server terbaru (Pagination)...")
    
    while True:
        try:
            # Menggunakan endpoint index.php dengan parameter page & lang
            url = f"{BASE_HOME_API}?page={page}&lang=in"
            resp = get_with_retry(url, timeout=20).json()
            
            # Helper untuk mendapatkan list data
            items = []
            if isinstance(resp, dict):
                 if resp.get("status") is True and "data" in resp:
                     items = resp["data"]
                 elif "data" in resp: # Fallback
                     items = resp["data"]
            elif isinstance(resp, list):
                items = resp

            if not items:
                print(f"   Page {page}: Kosong (Stop Scan).")
                break
            
            new_count = 0
            for item in items:
                # Mapping field sesuai JSON streamapi
                bid = str(item.get('id', item.get('bookId')))
                if bid not in seen_ids:
                    seen_ids.add(bid)
                    
                    # Parse episode string "74 Episode" -> int 74
                    eps_str = str(item.get('episode', '0'))
                    try:
                        eps_count = int(''.join(filter(str.isdigit, eps_str)))
                    except:
                        eps_count = 0
                        
                    book_obj = {
                        'id': bid,
                        'name': item.get('title', item.get('bookName')),
                        'chapterCount': eps_count,
                        'cover': item.get('thumbnail', item.get('coverWap'))
                    }
                    all_books.append(book_obj)
                    new_count += 1
            
            print(f"   Page {page}: Dapat {len(items)} item. ({new_count} baru)", end="\r")
            
            # Jika di page ini tidak ada data baru (kemungkinan duplikat/looping), stop
            if new_count == 0 and len(items) > 0:
                 print(f"\n   [Info] Tidak ada data baru di page {page}. Stop.")
                 break
            
            page += 1
                
        except Exception as e:
            print(f"\n   [Error] Gagal fetch page {page}: {e}")
            break
            
    print(f"\n   Selesai scan! Total unik: {len(all_books)} buku.")
    return all_books

def menu_download_auto_all():
    # 1. Fetch
    all_books = fetch_all_available_books()
    if not all_books: return

    # 2. Filter
    queue = []
    print("\nMemfilter history...")
    for b in all_books:
        bid = str(b['id'])
        hist = check_is_downloaded(bid)
        if hist is None or int(b.get('chapterCount', 0)) > int(hist.get("TotalChapters", 0)):
            queue.append(b)

    if not queue:
        print("Semua sudah up-to-date!")
        return

    print(f"\nDitemukan {len(queue)} buku yang perlu didownload.")

    # 3. Hitung Size (Opsional tapi diminta user)
    print("\nMenghitung total ukuran file (Ctrl+C untuk Skip)...")
    total_size = 0
    
    try:
        # Kita pakai Parallel juga biar hitung sizenya CEPAT!
        def get_book_size(book):
            b_size = 0
            try:
                bid = str(book['id'])
                url = f"{BASE_DOWNLOAD_API}?bookId={bid}"
                # Gunakan get_with_retry agar pakai header & verify=False
                r = get_with_retry(url, timeout=15).json()
                
                # Cek status success/true
                if r.get("success") or r.get("status") is True:
                     data_content = r.get("data", {})
                     # Support structure baru (chapters) & lama (chapterList)
                     chapter_list = data_content.get("chapters") or data_content.get("chapterList") or []
                     
                     for c in chapter_list:
                         # Hitung size video (mp4 / url)
                         vid_url = c.get('mp4') or c.get('url')
                         if vid_url:
                             b_size += get_file_size(vid_url)
                         
                         # Hitung size cover (cover / thumbnail)
                         cov_url = c.get('cover') or c.get('thumbnail')
                         if cov_url:
                             b_size += get_file_size(cov_url)
            except Exception: 
                pass
            return b_size

        with ThreadPoolExecutor(max_workers=10) as executor: # 10 threads untuk cek size
            futures = {executor.submit(get_book_size, b): b for b in queue}
            for future in tqdm(as_completed(futures), total=len(queue), desc="Checking Size"):
                total_size += future.result()
                
    except KeyboardInterrupt:
        print("\n   [Info] Skip cek size.")
    
    print(f"\nTOTAL QUEUE: {len(queue)} Buku.")
    print(f"ESTIMATED SIZE: {format_size(total_size)}")
    
    if input("Lanjut Download? (y/n): ").lower() != 'y': return

    # 4. Execute
    start = time.time()
    for i, book in enumerate(queue):
        print(f"\n--- [{i+1}/{len(queue)}] {book['name']} ---")
        process_book_parallel(book)
    
    print(f"\nSelesai dalam {(time.time()-start)/60:.2f} menit.")

def download_specific_chapter():
    """Download hanya SATU episode spesifik berdasarkan Book ID dan Chapter Index."""
    print("\n--- DOWNLOAD SPESIFIK EPISODE ---")
    book_id = input("Masukkan Book ID: ").strip()
    if not book_id: return

    try:
        idx_input = input("Masukkan Chapter Index (mulai dari 0, atau 1, 2, ...): ").strip()
        target_index = int(idx_input)
    except ValueError:
        print("Index harus angka.")
        return

    # 1. Fetch Data
    print("Mengambil data buku...")
    try:
        url = f"{BASE_DOWNLOAD_API}?bookId={book_id}"
        response = get_with_retry(url, timeout=20)
        json_resp = response.json()
    except Exception as e:
        print(f"Error API: {e}")
        return

    if not json_resp.get("success"):
        print("Buku tidak ditemukan atau API error.")
        return

    data_content = json_resp.get("data", {})
    chapters = data_content.get("chapterList", [])
    
    if not chapters:
        print("Tidak ada chapter di buku ini.")
        return
        
    # 2. Cari Chapter yang dimau
    target_chapter = None
    for chap in chapters:
        # API baru pakai 'index' integer 0, 1, 2...
        if int(chap.get("index", -1)) == target_index:
            target_chapter = chap
            break
    
    if not target_chapter:
        print(f"Chapter dengan index {target_index} tidak ditemukan.")
        return

    # 3. Download
    c_id = target_chapter.get("id")
    c_idx = target_chapter.get("index") # atau indexStr
    
    # Url video & cover
    vid_url = target_chapter.get("mp4")
    cover_url = target_chapter.get("cover")
    
    # Buat folder manual
    # Buat folder manual (Relative to Script Directory)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_name = os.path.join(script_dir, f"downloads_{book_id}_manual")
    if not os.path.exists(folder_name): os.makedirs(folder_name)
    
    print(f"\nFolder: '{folder_name}'")
    
    if vid_url:
        fname = f"{book_id}_{c_id}.mp4"
        print(f"Downloading Video: {fname} ...")
        download_file(vid_url, fname, folder_name)
        
    if cover_url:
        fname = f"{book_id}_{c_id}.jpg"
        print(f"Downloading Cover: {fname} ...")
        download_file(cover_url, fname, folder_name)
        
    print("Selesai.")

def menu_download_all_videos_covers_force():
    """Menu khusus untuk download semua video dan cover tanpa filter history."""
    print("\n--- DOWNLOAD ALL VIDEO & COVERS (MASS SCAN) ---")
    
    # 1. Fetch data dari server
    all_books = fetch_all_available_books()
    if not all_books: return

    print(f"\n{len(all_books)} buku terdeteksi.")
    print("Mode ini akan melewati pengecekan history Excel.")
    print("Script akan mengecek folder/file secara langsung (Resume jika ada).")

    if input("Mulai Download Semua? (y/n): ").lower() != 'y': return

    start = time.time()
    for i, book in enumerate(all_books):
        print(f"\n[Proses {i+1}/{len(all_books)}] Buku: {book['name']}")
        process_book_parallel(book)
    
    print(f"\nBatch Download Selesai dalam {(time.time()-start)/60:.2f} menit.")

def main():
    while True:
        print("\n=== DRAMABOX TURBO DOWNLOADER (MULTI-THREAD) ===")
        print("1. Manual ID (Full Book)")
        print("2. Auto Scan & Download All (Smart Filter)")
        print("3. Cek History Excel")
        print("4. Repair/Download Spesifik Episode")
        print("5. Download All Video & Cover (Force Check)")
        print("6. Exit")
        c = input("Pilih: ")
        
        if c=='1': 
            bid = input("ID: ")
            if bid: process_book_parallel({'id': bid, 'name': 'Manual'})
        elif c=='2': menu_download_auto_all()
        elif c=='3': 
            d = load_history()
            print(d.tail(10).to_string(index=False) if not d.empty else "Empty")
        elif c=='4':
            download_specific_chapter()
        elif c=='5':
            menu_download_all_videos_covers_force()
        elif c=='6': break

if __name__ == "__main__":
    main()
