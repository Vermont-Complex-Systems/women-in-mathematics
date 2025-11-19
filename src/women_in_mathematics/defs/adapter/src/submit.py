# src/women_in_mathematics/defs/adapter/src/submit.py

"""
Submit Women in Mathematics dataset to Storywrangler API

Reads prepared data from output/ and POSTs to API endpoints.
"""

import json
import requests
from pathlib import Path
from typing import Optional


class WomenInMathSubmitter:
    
    def __init__(self, output_dir: Path, api_url: str = "http://localhost:8000"):
        self.output_dir = Path(output_dir)
        self.api_url = api_url
        self.dataset_id = "women-in-math"
    
    def submit_metadata(self) -> bool:
        """Submit dataset metadata to /api/women-in-math"""
        
        dataset_file = self.output_dir / "dataset.json"
        
        if not dataset_file.exists():
            print(f"❌ Dataset file not found: {dataset_file}")
            print("   Run prepare.py first!")
            return False
        
        print(f"📤 Submitting metadata to {self.api_url}/api/{self.dataset_id}")
        
        with open(dataset_file) as f:
            dataset = json.load(f)
        
        try:
            response = requests.post(
                f"{self.api_url}/api/{self.dataset_id}",
                json=dataset,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Success! {result.get('authors', 0)} authors submitted")
                return True
            else:
                print(f"❌ Failed: {response.status_code}")
                print(f"   {response.text}")
                return False
                
        except requests.exceptions.ConnectionError:
            print(f"❌ Could not connect to {self.api_url}")
            print(f"   Is the API running? (uvicorn app.main:app --reload)")
            return False
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def submit_texts(self) -> int:
        """Submit texts for ingestion to /api/women-in-math/ingest"""
        
        texts_file = self.output_dir / "texts.json"
        
        if not texts_file.exists():
            print(f"⚠️  No texts file found: {texts_file}")
            return 0
        
        with open(texts_file) as f:
            data = json.load(f)
            texts = data.get("texts", [])
        
        if not texts:
            print("⚠️  No texts to ingest")
            return 0
        
        print(f"\n📤 Ingesting {len(texts)} texts to {self.api_url}/api/{self.dataset_id}/ingest")
        
        success_count = 0
        failed_count = 0
        
        for idx, text_data in enumerate(texts, 1):
            entity_id = text_data["entity_id"]
            text = text_data["text"]
            
            print(f"[{idx}/{len(texts)}] {entity_id[:50]}...", end=" ")
            
            try:
                response = requests.post(
                    f"{self.api_url}/api/{self.dataset_id}/ingest",
                    json={
                        "entity_id": entity_id,
                        "text": text
                    },
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    result = response.json()
                    ngrams = result.get("ngrams_extracted", 0)
                    print(f"✅ ({ngrams} n-grams)")
                    success_count += 1
                else:
                    print(f"❌ {response.status_code}")
                    failed_count += 1
                    
            except Exception as e:
                print(f"❌ {e}")
                failed_count += 1
        
        print(f"\n✅ Ingested: {success_count}/{len(texts)}")
        if failed_count > 0:
            print(f"⚠️  Failed: {failed_count}")
        
        return success_count
    
    def submit_all(self):
        """Submit both metadata and texts"""
        
        print("🚀 Submitting Women in Mathematics dataset to Storywrangler\n")
        
        # Step 1: Submit metadata
        if not self.submit_metadata():
            print("\n❌ Metadata submission failed. Aborting.")
            return
        
        # Step 2: Submit texts
        ingested = self.submit_texts()
        
        print(f"\n✅ Submission complete!")
        print(f"\n🔍 View your data:")
        print(f"  - Dataset: {self.api_url}/api/{self.dataset_id}")
        print(f"  - Authors: {self.api_url}/api/{self.dataset_id}/authors")
        print(f"  - N-grams: {self.api_url}/api/{self.dataset_id}/ngrams")


def main():
    """Run the submitter"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description="Submit Women in Mathematics dataset")
    parser.add_argument(
        "--api-url",
        default="http://localhost:8000",
        help="Storywrangler API URL (default: http://localhost:8000)"
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Only submit metadata, skip text ingestion"
    )
    parser.add_argument(
        "--texts-only",
        action="store_true",
        help="Only submit texts, skip metadata"
    )
    
    args = parser.parse_args()
    
    output_dir = Path(__file__).parent.parent / "output"
    submitter = WomenInMathSubmitter(output_dir, args.api_url)
    
    if args.metadata_only:
        submitter.submit_metadata()
    elif args.texts_only:
        submitter.submit_texts()
    else:
        submitter.submit_all()


if __name__ == "__main__":
    main()