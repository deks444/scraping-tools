import os
import requests
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

# Load environment variables
load_dotenv()

class DramaboxScraper:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("DRAMABOX_API_KEY")
        self.base_url = "https://streamapi.web.id/api-dramabox/"
        self.history_file = "download_history.json"
        self.master_excel = "dramabox_master_list.xlsx"
        self.download_dir = "downloads"
        self.history = self._load_history()
        
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

    def _load_history(self) -> Dict[str, Any]:
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r') as f:
                    return json.load(f)
            except:
                return {"downloaded_drama_ids": [], "downloaded_episode_ids": []}
        return {"downloaded_drama_ids": [], "downloaded_episode_ids": []}

    def _save_history(self):
        with open(self.history_file, 'w') as f:
            json.dump(self.history, f, indent=4)

    def _get(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}{endpoint}.php"
        if params is None:
            params = {}
        params['api_key'] = self.api_key
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if data.get('success'):
                return data.get('data')
            else:
                print(f"Error from API: {data.get('message')}")
        except Exception as e:
            print(f"Request failed: {e}")
        return None

    def get_drama_list(self, page: int = 1, page_size: int = 20, lang: str = "in") -> tuple[List[Dict[str, Any]], bool]:
        data = self._get("new", {"page": page, "pageSize": page_size, "lang": lang})
        if data:
            return data.get('list', []), data.get('isMore', False)
        return [], False

    def get_drama_detail(self, drama_id: str, lang: str = "in") -> Optional[Dict[str, Any]]:
        return self._get("drama", {"id": drama_id, "lang": lang})

    def get_watch_info(self, drama_id: str, index: int, lang: str = "in") -> Optional[Dict[str, Any]]:
        return self._get("watch", {"id": drama_id, "index": index, "lang": lang, "source": "search_result"})

    def download_file(self, url: str, folder: str, filename: str) -> bool:
        if not os.path.exists(folder):
            os.makedirs(folder)
        
        filepath = os.path.join(folder, filename)
        if os.path.exists(filepath):
            print(f"File already exists: {filepath}")
            return True
        
        try:
            print(f"Downloading: {filename}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        except Exception as e:
            print(f"Download failed for {url}: {e}")
            return False

    def save_to_excel(self, dramas: List[Dict[str, Any]], filename: str = None):
        filename = filename or self.master_excel
        df = pd.DataFrame(dramas)
        df.to_excel(filename, index=False)
        print(f"Data saved to {filename}")

    def update_master_excel(self, drama_info: Dict[str, Any]):
        """Update a single drama record in the master excel file."""
        df = None
        if os.path.exists(self.master_excel):
            try:
                df = pd.read_excel(self.master_excel)
            except:
                pass
        
        required_columns = ["ID", "Title", "Introduction", "Tags", "Episodes Downloaded", "Total Episodes (API)", "Last Updated"]
        if df is None:
            df = pd.DataFrame(columns=required_columns)
        else:
            for col in required_columns:
                if col not in df.columns:
                    df[col] = "" # Add missing column

        # Ensure ID is string for comparison
        df['ID'] = df['ID'].astype(str)
        drama_id = str(drama_info['ID'])
        
        # Check if exists
        mask = df['ID'] == drama_id
        if mask.any():
            for key, value in drama_info.items():
                if key in df.columns:
                    df.loc[mask, key] = value
            df.loc[mask, "Last Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            new_row = drama_info.copy()
            new_row["Last Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        df.to_excel(self.master_excel, index=False)

    def get_excel_history(self) -> Dict[str, int]:
        """Returns a mapping of Drama ID to Episodes Downloaded from Excel."""
        if not os.path.exists(self.master_excel):
            return {}
        try:
            df = pd.read_excel(self.master_excel)
            df['ID'] = df['ID'].astype(str)
            return dict(zip(df['ID'], df['Episodes Downloaded']))
        except:
            return {}

    def download_drama(self, drama_id: str, lang: str = "in", only_new: bool = False):
        detail = self.get_drama_detail(drama_id, lang)
        if not detail:
            return
        
        drama_name = "".join(x for x in detail['bookName'] if x.isalnum() or x in " -_").strip()
        drama_folder = os.path.join(self.download_dir, f"{drama_id}_{drama_name}")
        
        if not os.path.exists(drama_folder):
            os.makedirs(drama_folder)

        # Download Cover
        if detail.get('cover'):
            self.download_file(detail['cover'], drama_folder, "cover.jpg")

        episodes = detail.get('chapterList', [])
        total_episodes = len(episodes)
        print(f"Found {total_episodes} episodes for '{drama_name}'")

        new_episodes_found = False
        downloaded_count = 0
        
        # Count existing files first to be accurate
        for f in os.listdir(drama_folder):
            if f.startswith("episode_") and f.endswith(".mp4"):
                downloaded_count += 1

        for ep in episodes:
            ep_index = ep['chapterIndex']
            ep_id = ep['chapterId']
            
            if only_new and ep_id in self.history['downloaded_episode_ids']:
                continue

            watch_info = self.get_watch_info(drama_id, ep_index, lang)
            if watch_info and watch_info.get('videoUrl'):
                filename = f"episode_{ep_index + 1}.mp4"
                success = self.download_file(watch_info['videoUrl'], drama_folder, filename)
                if success:
                    new_episodes_found = True
                    downloaded_count += 1
                    if ep_id not in self.history['downloaded_episode_ids']:
                        self.history['downloaded_episode_ids'].append(ep_id)
        
        # Update Master Excel
        tags = detail.get('tags', [])
        tags_str = ", ".join(tags) if isinstance(tags, list) else str(tags)
        
        self.update_master_excel({
            "ID": drama_id,
            "Title": detail.get('bookName', ''),
            "Introduction": detail.get('introduction', detail.get('bookIntroduction', '')),
            "Tags": tags_str,
            "Episodes Downloaded": downloaded_count,
            "Total Episodes (API)": total_episodes
        })

        if new_episodes_found or drama_id not in self.history['downloaded_drama_ids']:
            if drama_id not in self.history['downloaded_drama_ids']:
                self.history['downloaded_drama_ids'].append(drama_id)
            self._save_history()
        else:
            print(f"No new episodes for '{drama_name}'")

    def download_all(self, lang: str = "in", only_new: bool = False):
        excel_history = self.get_excel_history() if only_new else {}
        page = 1
        has_more = True
        
        while has_more:
            print(f"Fetching page {page}...")
            dramas, has_more = self.get_drama_list(page=page, lang=lang)
            if not dramas:
                break
            
            for drama in dramas:
                drama_id = str(drama['bookId'])
                api_ep_count = drama['chapterCount']
                
                if only_new and drama_id in excel_history:
                    downloaded_so_far = excel_history[drama_id]
                    if api_ep_count <= downloaded_so_far:
                        print(f"Skipping '{drama['bookName']}' (Already up to date: {downloaded_so_far} eps)")
                        continue
                    else:
                        print(f"Update found for '{drama['bookName']}': {downloaded_so_far} -> {api_ep_count} eps")

                print(f"Processing Drama: {drama['bookName']}")
                self.download_drama(drama_id, lang, only_new=only_new)
            
            page += 1
        print("Download process finished.")

    def download_single_episode(self, drama_id: str, episode_index: int, lang: str = "in"):
        detail = self.get_drama_detail(drama_id, lang)
        if not detail:
            return
            
        drama_name = "".join(x for x in detail['bookName'] if x.isalnum() or x in " -_").strip()
        drama_folder = os.path.join(self.download_dir, f"{drama_id}_{drama_name}")
        
        watch_info = self.get_watch_info(drama_id, episode_index, lang)
        if watch_info and watch_info.get('videoUrl'):
            filename = f"episode_{episode_index + 1}.mp4"
            self.download_file(watch_info['videoUrl'], drama_folder, filename)
            
            # Record in history
            episodes = detail.get('chapterList', [])
            target_ep = next((e for e in episodes if e['chapterIndex'] == episode_index), None)
            if target_ep:
                ep_id = target_ep['chapterId']
                if ep_id not in self.history['downloaded_episode_ids']:
                    self.history['downloaded_episode_ids'].append(ep_id)
                    self._save_history()

    def sync_local_folders(self):
        """Scan folders and update the master excel file."""
        print(f"Scanning folder: {self.download_dir}...")
        if not os.path.exists(self.download_dir):
            print("Download folder not found.")
            return

        folders = [f for f in os.listdir(self.download_dir) if os.path.isdir(os.path.join(self.download_dir, f))]
        
        for folder_name in folders:
            if "_" in folder_name:
                parts = folder_name.split("_", 1)
                drama_id = parts[0]
                drama_title = parts[1]
            else:
                drama_id = "Unknown"
                drama_title = folder_name
            
            if drama_id == "Unknown": continue

            folder_path = os.path.join(self.download_dir, folder_name)
            ep_count = len([f for f in os.listdir(folder_path) if f.endswith(".mp4")])
            
            # Update Master Excel from local scan
            self.update_master_excel({
                "ID": drama_id,
                "Title": drama_title,
                "Episodes Downloaded": ep_count,
                "Total Episodes (API)": ep_count # We don't know API count from local scan
            })
            
            if drama_id not in self.history['downloaded_drama_ids']:
                self.history['downloaded_drama_ids'].append(drama_id)

        self._save_history()
        print(f"Sync complete. Check {self.master_excel}")

    def export_drama_to_excel_with_urls(self, drama_id: str, lang: str = "in"):
        """Fetch drama metadata and all episode URLs, then export to a separate Excel file."""
        print(f"Fetching details for Drama ID: {drama_id}...")
        detail = self.get_drama_detail(drama_id, lang)
        if not detail:
            print(f"Failed to get details for Drama ID: {drama_id}")
            return
        
        drama_name = detail.get('bookName', f'Drama_{drama_id}')
        episodes = detail.get('chapterList', [])
        tags = detail.get('tags', [])
        tags_str = ", ".join(tags) if isinstance(tags, list) else str(tags)
        intro = detail.get('introduction', detail.get('bookIntroduction', ''))
        
        data_list = []
        print(f"Fetching video URLs for {len(episodes)} episodes... This may take a moment.")
        
        for ep in episodes:
            ep_index = ep['chapterIndex']
            print(f"  Fetching URL for Episode {ep_index + 1}/{len(episodes)}...", end='\r')
            watch_info = self.get_watch_info(drama_id, ep_index, lang)
            
            data_list.append({
                "Drama ID": drama_id,
                "Drama Title": drama_name,
                "Introduction": intro,
                "Tags": tags_str,
                "Episode Index": ep_index + 1,
                "Episode Title": ep.get('chapterName', f'Episode {ep_index + 1}'),
                "Video URL": watch_info.get('videoUrl', 'Not Found') if watch_info else 'Error'
            })
        
        print(f"\nFetched {len(data_list)} episode URLs.")
        
        # Save to a specific file for this drama
        safe_name = "".join(x for x in drama_name if x.isalnum() or x in " -_").strip()
        export_filename = f"drama_info_{drama_id}_{safe_name}.xlsx"
        
        df = pd.DataFrame(data_list)
        df.to_excel(export_filename, index=False)
        print(f"Detailed drama info with URLs saved to: {export_filename}")

    def export_all_dramas_to_excel_with_urls(self, lang: str = "in"):
        """Iterate through all pages and export every drama's info and episode URLs to one Excel."""
        page = 1
        has_more = True
        all_data_list = []
        
        print("Starting global export of all dramas with video URLs...")
        
        while has_more:
            print(f"\n--- Fetching Page {page} ---")
            dramas, has_more = self.get_drama_list(page=page, lang=lang)
            if not dramas:
                break
            
            for drama in dramas:
                drama_id = str(drama['bookId'])
                print(f"Processing Drama: {drama['bookName']} (ID: {drama_id})")
                
                detail = self.get_drama_detail(drama_id, lang)
                if not detail:
                    print(f"  Skipping ID {drama_id}: Could not fetch details.")
                    continue
                
                episodes = detail.get('chapterList', [])
                tags = detail.get('tags', [])
                tags_str = ", ".join(tags) if isinstance(tags, list) else str(tags)
                intro = detail.get('introduction', detail.get('bookIntroduction', ''))
                
                for ep in episodes:
                    ep_index = ep['chapterIndex']
                    print(f"    Fetching URL for Episode {ep_index + 1}/{len(episodes)}...", end='\r')
                    watch_info = self.get_watch_info(drama_id, ep_index, lang)
                    
                    all_data_list.append({
                        "Drama ID": drama_id,
                        "Drama Title": detail.get('bookName', ''),
                        "Introduction": intro,
                        "Tags": tags_str,
                        "Episode Index": ep_index + 1,
                        "Episode Title": ep.get('chapterName', f'Episode {ep_index + 1}'),
                        "Video URL": watch_info.get('videoUrl', 'Not Found') if watch_info else 'Error'
                    })
                print(f"\n  Done: {len(episodes)} episodes added.")
            
            page += 1
        
        if all_data_list:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            export_filename = f"dramabox_all_urls_{timestamp}.xlsx"
            print(f"\nSaving {len(all_data_list)} total rows to Excel...")
            df = pd.DataFrame(all_data_list)
            df.to_excel(export_filename, index=False)
            print(f"Global export complete: {export_filename}")
        else:
            print("No data found to export.")

def main():
    scraper = DramaboxScraper()
    
    while True:
        print("\n=== Dramabox Scraper Menu ===")
        print("1. Download All (Videos, Covers, Excel)")
        print("2. Download 1 Drama (All Episodes)")
        print("3. Download 1 Specific Episode")
        print("4. Check Update (Download only new items)")
        print("5. Sync Folders to Excel & Refresh History")
        print("6. Export ALL Dramas + Video URLs (Loop all pages)")
        print("7. Exit")
        
        choice = input("Enter choice (1-7): ")
        
        if choice == '1':
            scraper.download_all()
        elif choice == '2':
            drama_id = input("Enter Drama ID: ")
            scraper.download_drama(drama_id)
        elif choice == '3':
            drama_id = input("Enter Drama ID: ")
            ep_idx = input("Enter Episode Index (starting from 0): ")
            if ep_idx.isdigit():
                scraper.download_single_episode(drama_id, int(ep_idx))
        elif choice == '4':
            scraper.download_all(only_new=True)
        elif choice == '5':
            scraper.sync_local_folders()
        elif choice == '6':
            scraper.export_all_dramas_to_excel_with_urls()
        elif choice == '7':
            break
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()
