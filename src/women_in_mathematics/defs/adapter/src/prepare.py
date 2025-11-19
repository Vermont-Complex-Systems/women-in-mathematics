# src/women_in_mathematics/defs/adapter/src/prepare.py

"""
Women in Mathematics Adapter

Prepares data for submission to Storywrangler API.
"""

import pandas as pd
import json
from pathlib import Path
from typing import Optional, Dict, List
import requests
from storywrangler.validation import EntityValidator
from pyprojroot.here import here

class WomenInMathAdapter:
    
    def __init__(self, data_dir: Path, text_dir: Path, output_dir: Path):
        self.data_dir = Path(data_dir)
        self.text_dir = Path(text_dir)
        self.output_dir = Path(output_dir)
        self.validator = EntityValidator()
        self.personal = pd.read_csv(self.data_dir / "personal.csv")
        
        # Build text file mapping
        self.text_files = self._build_text_mapping()
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def _build_text_mapping(self) -> Dict[str, Path]:
        """
        Build mapping from full_name to text file
        
        Text files are named like: adams_rachel.pdf.txt
        """
        mapping = {}
        
        # Get all text files (including .pdf.txt)
        text_files = list(self.text_dir.glob("*.txt"))
        
        print(f"📁 Found {len(text_files)} text files in {self.text_dir}")
        
        if len(text_files) > 0:
            print(f"   Example: {text_files[0].name}")
        
        # Try to match each person to a text file
        for _, person in self.personal.iterrows():
            name = person['full_name']
            
            # Generate candidates: lastname_firstname format
            # "Rachel Adams" -> "adams_rachel"
            name_parts = name.split()
            
            candidates = []
            
            # Try lastname_firstname
            if len(name_parts) >= 2:
                # Handle cases like "Sister Mary Nicholas Arnoldy"
                # Try: arnoldy_sister_mary_nicholas, arnoldy_mary_nicholas, arnoldy_nicholas
                lastname = name_parts[-1].lower()
                firstnames = [p.lower() for p in name_parts[:-1]]
                
                # Try all combinations
                candidates.append(f"{lastname}_{'_'.join(firstnames)}")
                if len(firstnames) > 1:
                    candidates.append(f"{lastname}_{firstnames[-1]}")  # Just first name
                    candidates.append(f"{lastname}_{firstnames[0]}")   # Just first word
            
            # Also try firstname_lastname
            if len(name_parts) >= 2:
                firstname = name_parts[0].lower()
                lastname = name_parts[-1].lower()
                candidates.append(f"{firstname}_{lastname}")
            
            # Try full name as-is (lowercase, underscores)
            candidates.append(name.lower().replace(" ", "_").replace(".", "").replace(",", ""))
            
            # Clean up candidates (remove special chars)
            candidates = [
                c.replace("(", "").replace(")", "").replace(".", "")
                for c in candidates
            ]
            
            # Check each candidate against text files
            for candidate in candidates:
                for text_file in text_files:
                    # Remove .pdf.txt or .txt extension
                    stem = text_file.name.replace(".pdf.txt", "").replace(".txt", "")
                    
                    if stem.lower() == candidate.lower():
                        mapping[name] = text_file
                        break
                
                if name in mapping:
                    break
        
        print(f"✅ Matched {len(mapping)}/{len(self.personal)} people to text files\n")
        
        # Show some examples of matches/misses
        if len(mapping) > 0:
            print("   Example matches:")
            for name, path in list(mapping.items())[:3]:
                print(f"     {name} → {path.name}")
        
        if len(mapping) < len(self.personal):
            unmatched = [
                person['full_name'] 
                for _, person in self.personal.iterrows() 
                if person['full_name'] not in mapping
            ]
            print(f"\n   ⚠️  {len(unmatched)} unmatched people:")
            for name in unmatched[:5]:
                print(f"     - {name}")
            if len(unmatched) > 5:
                print(f"     ... and {len(unmatched) - 5} more")
        
        print()
        
        return mapping
    
    def lookup_wikidata(self, name: str) -> Optional[str]:
        """Look up Wikidata Q-code via API"""
        try:
            url = "https://www.wikidata.org/w/api.php"
            params = {
                "action": "wbsearchentities",
                "format": "json",
                "language": "en",
                "type": "item",
                "search": name,
                "limit": 3
            }
            headers = {
                "User-Agent": "WomenInMathAdapter/0.1 (https://github.com/Vermont-Complex-Systems/women-in-mathematics)"
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            if response.status_code != 200:
                return None
            
            results = response.json().get("search", [])
            
            for result in results:
                desc = result.get("description", "").lower()
                if "mathematician" in desc:
                    qid = result["id"]
                    print(f"  ✓ {qid}")
                    return qid
            
            return None
            
        except Exception as e:
            print(f"  ⚠️  {e}")
            return None
    
    def map_entity(self, person: pd.Series) -> Dict:
        """Map person to entity identifiers"""
        name = person['full_name']
        
        # Try Wikidata lookup
        wikidata_qid = self.lookup_wikidata(name)
        
        if wikidata_qid:
            entity_id = f"wikidata:{wikidata_qid}"
            confidence = 0.8
            
            # Keep local ID as alternate
            local_id = name.lower().replace(" ", "_").replace(".", "").replace(",", "").replace("(", "").replace(")", "")
            entity_ids = [f"local:women-in-math:{local_id}"]
        else:
            # Use local identifier as primary
            local_id = name.lower().replace(" ", "_").replace(".", "").replace(",", "").replace("(", "").replace(")", "")
            entity_id = f"local:women-in-math:{local_id}"
            confidence = 0.5
            entity_ids = None
        
        # Validate
        if not self.validator.validate(entity_id):
            raise ValueError(f"Invalid entity_id: {entity_id}")
        
        if entity_ids:
            for eid in entity_ids:
                if not self.validator.validate(eid):
                    raise ValueError(f"Invalid alternate entity_id: {eid}")
        
        return {
            "entity_id": entity_id,
            "entity_ids": entity_ids,
            "confidence": confidence
        }
    
    def get_text(self, name: str) -> Optional[str]:
        """Get biography text using pre-built mapping"""
        text_file = self.text_files.get(name)
        
        if text_file and text_file.exists():
            return text_file.read_text()
        
        return None
    
    def prepare(self) -> tuple[Dict, List[Dict]]:
        """Prepare dataset for submission"""
        print("🔧 Preparing Women in Mathematics dataset\n")
        
        authors = []
        texts = []
        
        for idx, person in self.personal.iterrows():
            name = person['full_name']
            print(f"[{idx+1}/{len(self.personal)}] {name}")
            
            try:
                # Map to entity ID
                mapping = self.map_entity(person)
                
                # Get biographical data
                author = {
                    "entity_id": mapping["entity_id"],
                    "entity_ids": mapping["entity_ids"],
                    "entity_type": "person",
                    "confidence": mapping["confidence"],
                    "name": name,
                    "biographical_data": {
                        "birth_year": int(person['birthyear']) if pd.notna(person['birthyear']) else None,
                        "death_year": int(person['deathyear']) if pd.notna(person['deathyear']) else None,
                        "birthplace": person.get('birthplace'),
                        "field": "wikidata:Q395"
                    }
                }
                
                authors.append(author)
                
                # Get text
                text = self.get_text(name)
                if text:
                    print(f"  ✓ Text: {len(text)} chars")
                    texts.append({
                        "entity_id": mapping["entity_id"],
                        "text": text
                    })
                
                print()
                
            except Exception as e:
                print(f"  ❌ Error: {e}\n")
                continue
        
        # Create dataset submission
        dataset = {
            "dataset_id": "women-in-math",
            "name": "Women in Mathematics",
            "specification_version": "0.0.1",
            "description": "Biographical data of women mathematicians from 1800s-1900s",
            "authors": authors
        }
        
        return dataset, texts
    
    def save(self, dataset: Dict, texts: List[Dict]):
        """Save prepared data to output/"""
        
        # Save dataset metadata
        dataset_file = self.output_dir / "dataset.json"
        with open(dataset_file, 'w') as f:
            json.dump(dataset, f, indent=2)
        
        # Save texts for ingestion
        texts_file = self.output_dir / "texts.json"
        with open(texts_file, 'w') as f:
            json.dump({"texts": texts}, f, indent=2)
        
        print(f"✅ Prepared {len(dataset['authors'])} authors")
        print(f"✅ Found text for {len(texts)} authors")
        print(f"\n📁 Output:")
        print(f"  - Metadata: {dataset_file}")
        print(f"  - Texts: {texts_file}")
        
        # Summary
        wikidata_count = sum(1 for a in dataset['authors'] if a['entity_id'].startswith('wikidata:'))
        local_count = sum(1 for a in dataset['authors'] if a['entity_id'].startswith('local:'))
        
        print(f"\n📊 Entity Mapping:")
        print(f"  - Wikidata: {wikidata_count}")
        print(f"  - Local: {local_count}")
        
        print(f"\n🚀 Next Steps:")
        print(f"  1. Submit to API:")
        print(f"     uv run python src/women_in_mathematics/defs/adapter/src/submit.py")


def main():
    """Run the adapter"""
    
    # Paths relative to: src/women_in_mathematics/defs/adapter/src/prepare.py
    base_defs = Path(__file__).parent.parent.parent  # Go up to defs/
    
    data_dir = base_defs / "join" / "output"
    text_dir = base_defs / "extract" / "output"
    output_dir = Path(__file__).parent.parent / "output"  # adapter/output
    
    print(f"📁 Paths:")
    print(f"  Data: {data_dir}")
    print(f"  Text: {text_dir}")
    print(f"  Output: {output_dir}")
    print()
    
    # Check paths exist
    if not data_dir.exists():
        print(f"❌ Data directory not found: {data_dir}")
        return
    
    if not text_dir.exists():
        print(f"❌ Text directory not found: {text_dir}")
        return
    
    # Prepare
    adapter = WomenInMathAdapter(data_dir, text_dir, output_dir)
    dataset, texts = adapter.prepare()
    
    # Save
    adapter.save(dataset, texts)


if __name__ == "__main__":
    main()