import os
import threading
from icrawler.builtin import BingImageCrawler
from PIL import Image
import time

# Original 10 species + 6 new visually distinct species
species = [
    # Original 10
    "clownfish", "goldfish", "betta fish", "guppy", "angelfish",
    "zebrafish", "blue tang", "neon tetra", "lionfish", "discus",
    
    # 6 New visually distinct species
    "seahorse",           # Unique upright shape, horse-like head
    "pufferfish",         # Round, spiky when inflated
    "manta ray",          # Flat, wing-like body
    "moray eel",          # Long, snake-like body
    "parrotfish",         # Bright colors, beak-like mouth
    "swordfish"           # Long, pointed bill
]

def download_fish_images(fish_name, target_count=60):
    """
    Download images for a single fish species with error handling
    """
    try:
        print(f"🔥 [{threading.current_thread().name}] Starting download for: {fish_name}")
        
        # Create folder
        folder_path = f'fish_dataset/{fish_name}'
        os.makedirs(folder_path, exist_ok=True)
        
        # Download images (request more than needed to account for failures)
        crawler = BingImageCrawler(
            storage={'root_dir': folder_path},
            downloader_threads=4,  # Each crawler uses 4 threads
            log_level=40  # Only show errors
        )
        
        crawler.crawl(
            keyword=f"{fish_name} fish underwater",
            max_num=target_count + 20  # Request extra to ensure we get enough
        )
        
        # Verify and clean up broken images
        valid_count = 0
        for filename in os.listdir(folder_path):
            filepath = os.path.join(folder_path, filename)
            try:
                # Try to open image to verify it's valid
                with Image.open(filepath) as img:
                    img.verify()
                valid_count += 1
            except Exception:
                # Remove broken file
                try:
                    os.remove(filepath)
                except:
                    pass
        
        print(f"✅ [{fish_name}] Downloaded {valid_count} valid images")
        
    except Exception as e:
        print(f"❌ [{fish_name}] Error: {str(e)}")

def main():
    start_time = time.time()
    
    print("🚀 Starting multi-threaded fish image scraper...")
    print(f"📊 Downloading images for {len(species)} species with 12 concurrent threads\n")
    
    # Create thread pool
    threads = []
    
    # Launch threads (12 at once)
    for i in range(0, len(species), 12):
        batch = species[i:i+12]
        batch_threads = []
        
        for fish in batch:
            thread = threading.Thread(target=download_fish_images, args=(fish, 60))
            thread.start()
            batch_threads.append(thread)
        
        # Wait for this batch to complete before starting next
        for thread in batch_threads:
            thread.join()
    
    elapsed = time.time() - start_time
    
    print(f"\n🎉 All downloads complete in {elapsed:.2f} seconds!")
    print("\n📁 Dataset summary:")
    
    # Show summary with shape descriptions
    shape_descriptions = {
        "clownfish": "Oval body, orange/white stripes",
        "goldfish": "Round body, orange/red",
        "betta fish": "Long flowing fins",
        "guppy": "Small, colorful tail",
        "angelfish": "Triangular, tall body",
        "zebrafish": "Horizontal blue stripes",
        "blue tang": "Flat oval, bright blue",
        "neon tetra": "Tiny, iridescent stripe",
        "lionfish": "Spiky fins, striped",
        "discus": "Circular, flat body",
        "seahorse": "Upright, curled tail",
        "pufferfish": "Round, spiky",
        "manta ray": "Flat, wing-shaped",
        "moray eel": "Long, snake-like",
        "parrotfish": "Beak-like mouth, colorful",
        "swordfish": "Long pointed bill"
    }
    
    total_images = 0
    for fish in species:
        folder_path = f'fish_dataset/{fish}'
        if os.path.exists(folder_path):
            count = len(os.listdir(folder_path))
            total_images += count
            status = "✅" if count >= 50 else "⚠️"
            description = shape_descriptions.get(fish, "")
            print(f"{status} {fish}: {count} images - {description}")
    
    print(f"\n📊 Total images: {total_images}")
    print(f"⏱️  Average: {elapsed/len(species):.2f} seconds per species")

if __name__ == "__main__":
    main()