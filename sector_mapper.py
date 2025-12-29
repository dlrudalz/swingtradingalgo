import pandas as pd
import json
import os
import asyncio
import aiohttp
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SectorMappingUpdater:
    """Updates sector mapping by detecting and categorizing 'Other' sector tickers"""
    
    def __init__(self, ranking_dir: str, mapping_file: str):
        self.ranking_dir = ranking_dir
        self.mapping_file = mapping_file
        self.sector_mapping = {}
        self.major_sectors = []
        self.fallback_keywords = {}
        self.load_existing_mapping()
        
    def load_existing_mapping(self):
        """Load existing sector mapping from JSON file"""
        try:
            with open(self.mapping_file, 'r') as f:
                data = json.load(f)
                self.sector_mapping = data.get('sector_mapping', {})
                self.major_sectors = data.get('major_sectors', [])
                self.fallback_keywords = data.get('fallback_keywords', {})
            logger.info(f"✅ Loaded existing mapping: {len(self.sector_mapping)} mappings, {len(self.major_sectors)} major sectors")
        except Exception as e:
            logger.error(f"❌ Failed to load sector mapping: {e}")
            raise
    
    def save_mapping(self):
        """Save updated sector mapping to JSON file"""
        try:
            with open(self.mapping_file, 'w') as f:
                json.dump({
                    "sector_mapping": self.sector_mapping,
                    "major_sectors": self.major_sectors,
                    "fallback_keywords": self.fallback_keywords
                }, f, indent=2)
            logger.info(f"💾 Saved updated mapping to {self.mapping_file}")
        except Exception as e:
            logger.error(f"❌ Failed to save sector mapping: {e}")
            raise
    
    def find_other_sector_files(self) -> List[str]:
        """Find all CSV files in the ranking results sectors directory"""
        other_files = []
        try:
            if not os.path.exists(self.ranking_dir):
                logger.error(f"❌ Directory not found: {self.ranking_dir}")
                return other_files
            
            for filename in os.listdir(self.ranking_dir):
                if filename.endswith('_rankings.csv'):
                    filepath = os.path.join(self.ranking_dir, filename)
                    other_files.append(filepath)
            
            logger.info(f"📁 Found {len(other_files)} sector CSV files")
            return other_files
        except Exception as e:
            logger.error(f"❌ Error finding sector files: {e}")
            return []
    
    def extract_other_sector_tickers(self) -> List[Dict]:
        """Extract tickers from 'Other' sector files and other files with unmapped sectors"""
        all_tickers = []
        
        sector_files = self.find_other_sector_files()
        if not sector_files:
            return all_tickers
        
        for filepath in sector_files:
            try:
                df = pd.read_csv(filepath)
                logger.info(f"📊 Processing {os.path.basename(filepath)}: {len(df)} tickers")
                
                for _, row in df.iterrows():
                    ticker_data = {
                        'ticker': row.get('Ticker', ''),
                        'name': row.get('Name', ''),
                        'exchange': row.get('Exchange', ''),
                        'major_sector': row.get('Major_Sector', ''),
                        'detailed_sector': row.get('Detailed_Sector', ''),
                        'current_price': row.get('Current_Price', 0)
                    }
                    
                    # Check if this ticker is in "Other" sector or has unmapped detailed sector
                    if (ticker_data['major_sector'] == 'Other' or 
                        ticker_data['detailed_sector'] not in self.sector_mapping):
                        all_tickers.append(ticker_data)
                        
            except Exception as e:
                logger.error(f"❌ Error processing {filepath}: {e}")
                continue
        
        logger.info(f"🎯 Found {len(all_tickers)} tickers needing sector mapping")
        return all_tickers
    
    async def detect_sector_for_ticker(self, ticker_data: Dict) -> Tuple[str, str]:
        """Detect the correct sector for a ticker using multiple sources"""
        ticker = ticker_data['ticker']
        name = ticker_data['name']
        detailed_sector = ticker_data['detailed_sector']
        
        logger.info(f"🔍 Analyzing {ticker}: {name} | Current: {detailed_sector}")
        
        # Method 1: Use existing detailed sector with enhanced keyword matching
        if detailed_sector and detailed_sector != 'Unknown':
            major_sector = self.enhanced_keyword_mapping(detailed_sector)
            if major_sector != 'Other':
                logger.info(f"   ✅ Mapped via keyword: {detailed_sector} -> {major_sector}")
                return detailed_sector, major_sector
        
        # Method 2: Use company name for sector detection
        if name:
            name_sector = self.analyze_company_name(name, ticker)
            if name_sector != 'Other':
                logger.info(f"   ✅ Mapped via name analysis: {name} -> {name_sector}")
                # Create a mapping for the detailed sector based on name analysis
                new_detailed_sector = f"SERVICES-{name_sector.upper()} RELATED" if name_sector != 'Other' else detailed_sector
                return new_detailed_sector, name_sector
        
        # Method 3: Use external APIs (Polygon, Yahoo Finance, etc.)
        try:
            external_sector = await self.get_sector_from_external_sources(ticker)
            if external_sector and external_sector != 'Other':
                logger.info(f"   ✅ Mapped via external API: {ticker} -> {external_sector}")
                return f"EXTERNAL-{external_sector.upper()}", external_sector
        except Exception as e:
            logger.debug(f"   External API failed for {ticker}: {e}")
        
        # Method 4: Final fallback - analyze the business based on available info
        final_sector = self.final_fallback_analysis(ticker_data)
        logger.info(f"   🔄 Final mapping: {detailed_sector} -> {final_sector}")
        
        return detailed_sector, final_sector
    
    def enhanced_keyword_mapping(self, detailed_sector: str) -> str:
        """Enhanced keyword-based sector mapping"""
        if not detailed_sector or detailed_sector == 'Unknown':
            return 'Other'
        
        sector_lower = detailed_sector.lower()
        
        # Enhanced keyword mappings with more specific patterns
        keyword_mappings = [
            # Technology
            (['tech', 'software', 'computer', 'electronic', 'internet', 'semiconductor', 
              'computing', 'digital', 'data processing', 'programming', 'cloud', 'ai', 
              'artificial intelligence', 'machine learning', 'cybersecurity'], 'Technology'),
            
            # Health Care
            (['health', 'medical', 'pharma', 'bio', 'drug', 'hospital', 'care', 
              'biotech', 'pharmaceutical', 'therapy', 'clinical', 'diagnostic', 
              'healthcare', 'medical device', 'life sciences'], 'Health Care'),
            
            # Financials
            (['bank', 'finance', 'insurance', 'invest', 'credit', 'lending', 'mortgage',
              'financial', 'capital', 'asset management', 'investment', 'broker', 
              'wealth management', 'fintech'], 'Financials'),
            
            # Consumer Discretionary
            (['retail', 'consumer', 'auto', 'appliance', 'entertainment', 'travel', 
              'hotel', 'leisure', 'recreation', 'gaming', 'media', 'fashion', 
              'e-commerce', 'restaurant', 'luxury'], 'Consumer Discretionary'),
            
            # Consumer Staples
            (['food', 'beverage', 'grocery', 'staples', 'dairy', 'household', 
              'personal care', 'cosmetics', 'cleaning', 'packaged food', 'supermarket'], 'Consumer Staples'),
            
            # Industrials
            (['industrial', 'manufactur', 'machine', 'engine', 'construction', 
              'engineering', 'aerospace', 'defense', 'transportation', 'logistics', 
              'shipping', 'aviation', 'railroad', 'infrastructure'], 'Industrials'),
            
            # Energy
            (['oil', 'gas', 'energy', 'petroleum', 'drilling', 'crude', 'refining',
              'renewable', 'solar', 'wind', 'utilities', 'power generation'], 'Energy'),
            
            # Utilities
            (['electric', 'utility', 'water', 'power', 'sanitary', 'waste management',
              'renewable energy', 'grid', 'infrastructure'], 'Utilities'),
            
            # Real Estate
            (['real estate', 'property', 'reit', 'building', 'apartment', 'commercial',
              'residential', 'mortgage reit', 'realty', 'land', 'development'], 'Real Estate'),
            
            # Materials
            (['mining', 'metal', 'material', 'chemical', 'steel', 'plastic', 'rubber',
              'forest', 'paper', 'packaging', 'metals', 'minerals', 'composites'], 'Materials'),
            
            # Communication Services
            (['telecom', 'communication', 'media', 'broadcast', 'publishing', 'radio', 
              'tv', 'wireless', 'cable', 'entertainment', 'social media', 'streaming'], 'Communication Services'),
            
            # Specific service mappings
            (['help supply', 'staffing', 'employment', 'recruitment', 'human resources'], 'Industrials'),
            (['warehousing', 'storage', 'logistics', 'distribution'], 'Industrials'),
            (['consulting', 'business services', 'professional services'], 'Industrials'),
            (['education', 'training', 'educational services'], 'Consumer Discretionary'),
        ]
        
        for keywords, sector in keyword_mappings:
            if any(keyword in sector_lower for keyword in keywords):
                return sector
        
        return 'Other'
    
    def analyze_company_name(self, company_name: str, ticker: str) -> str:
        """Analyze company name to determine sector"""
        if not company_name:
            return 'Other'
        
        name_lower = company_name.lower()
        
        # Common company name patterns
        name_patterns = [
            # Technology patterns
            (['tech', 'software', 'systems', 'digital', 'cloud', 'data', 'network', 
              'computing', 'cyber', 'internet', 'web'], 'Technology'),
            
            # Health Care patterns
            (['pharma', 'bio', 'life', 'medical', 'health', 'care', 'therapy', 
              'biologics', 'diagnostics', 'hospital'], 'Health Care'),
            
            # Financial patterns
            (['bank', 'financial', 'capital', 'insurance', 'trust', 'credit', 
              'investment', 'asset', 'wealth', 'fund'], 'Financials'),
            
            # Industrial patterns
            (['industrial', 'manufacturing', 'engineering', 'construction', 
              'logistics', 'shipping', 'transport', 'aviation', 'marine'], 'Industrials'),
            
            # Consumer patterns
            (['retail', 'store', 'shop', 'outlet', 'market', 'brand', 'consumer',
              'apparel', 'fashion', 'auto', 'car'], 'Consumer Discretionary'),
            
            # Energy patterns
            (['energy', 'power', 'oil', 'gas', 'petroleum', 'renewable', 'solar',
              'wind'], 'Energy'),
            
            # Real Estate patterns
            (['realty', 'properties', 'reit', 'estate', 'development', 'building'], 'Real Estate'),
            
            # Materials patterns
            (['materials', 'chemical', 'steel', 'metal', 'mining', 'forest', 'paper'], 'Materials'),
        ]
        
        for patterns, sector in name_patterns:
            if any(pattern in name_lower for pattern in patterns):
                return sector
        
        return 'Other'
    
    async def get_sector_from_external_sources(self, ticker: str) -> str:
        """Get sector information from external APIs"""
        try:
            # Try Polygon.io first
            polygon_sector = await self.get_sector_from_polygon(ticker)
            if polygon_sector:
                return polygon_sector
            
            # Try Yahoo Finance as fallback
            yahoo_sector = await self.get_sector_from_yahoo(ticker)
            if yahoo_sector:
                return yahoo_sector
            
        except Exception as e:
            logger.debug(f"External sector detection failed for {ticker}: {e}")
        
        return 'Other'
    
    async def get_sector_from_polygon(self, ticker: str) -> Optional[str]:
        """Get sector from Polygon.io API"""
        try:
            api_key = "ld1Poa63U6t4Y2MwOCA2JeKQyHVrmyg8"  # Use your existing API key
            url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={api_key}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        result = data.get('results', {})
                        sector = result.get('sic_description') or result.get('industry_category')
                        if sector:
                            return self.map_polygon_sector(sector)
            
        except Exception as e:
            logger.debug(f"Polygon API failed for {ticker}: {e}")
        
        return None
    
    def map_polygon_sector(self, polygon_sector: str) -> str:
        """Map Polygon.io sector to our major sectors"""
        if not polygon_sector:
            return 'Other'
        
        sector_lower = polygon_sector.lower()
        
        mapping = {
            'technology': 'Technology',
            'health care': 'Health Care',
            'financial': 'Financials',
            'consumer discretionary': 'Consumer Discretionary',
            'consumer staples': 'Consumer Staples',
            'industrial': 'Industrials',
            'energy': 'Energy',
            'utilities': 'Utilities',
            'real estate': 'Real Estate',
            'materials': 'Materials',
            'communication services': 'Communication Services'
        }
        
        for poly_sector, our_sector in mapping.items():
            if poly_sector in sector_lower:
                return our_sector
        
        return 'Other'
    
    async def get_sector_from_yahoo(self, ticker: str) -> Optional[str]:
        """Get sector from Yahoo Finance (fallback)"""
        try:
            # This would require yfinance package
            # For now, return None - you can implement this if needed
            return None
        except Exception:
            return None
    
    def final_fallback_analysis(self, ticker_data: Dict) -> str:
        """Final fallback analysis when all other methods fail"""
        ticker = ticker_data['ticker']
        name = ticker_data['name']
        exchange = ticker_data['exchange']
        
        # Analyze based on exchange and naming patterns
        if exchange == 'XNAS' or exchange == 'XNYS':
            # US exchanges often have more tech companies
            if any(word in (name or '').lower() for word in ['inc', 'corp', 'holdings']):
                return 'Financials'  # Many holding companies are financials
        
        # Default to Industrials for service companies
        if 'services' in (ticker_data.get('detailed_sector') or '').lower():
            return 'Industrials'
        
        return 'Other'
    
    async def update_sector_mapping(self) -> Dict:
        """Main method to update sector mapping by analyzing 'Other' sector tickers"""
        logger.info("🚀 Starting sector mapping update process...")
        
        # Extract tickers that need sector mapping
        tickers_needing_mapping = self.extract_other_sector_tickers()
        if not tickers_needing_mapping:
            logger.info("✅ No tickers found needing sector mapping")
            return {}
        
        # Process each ticker to detect correct sector
        updates_made = {}
        successful_updates = 0
        
        for ticker_data in tickers_needing_mapping:
            try:
                detailed_sector = ticker_data['detailed_sector']
                current_major_sector = ticker_data['major_sector']
                
                # Skip if already properly mapped (not 'Other')
                if (detailed_sector in self.sector_mapping and 
                    self.sector_mapping[detailed_sector] != 'Other'):
                    continue
                
                # Detect correct sector
                new_detailed, new_major = await self.detect_sector_for_ticker(ticker_data)
                
                # Update mapping if we found a better sector
                if (new_major != 'Other' and new_major != current_major_sector and
                    new_detailed and new_detailed != 'Unknown'):
                    
                    self.sector_mapping[new_detailed] = new_major
                    updates_made[new_detailed] = new_major
                    successful_updates += 1
                    
                    logger.info(f"📝 Added mapping: '{new_detailed}' -> '{new_major}'")
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"❌ Error processing {ticker_data['ticker']}: {e}")
                continue
        
        # Save the updated mapping
        if updates_made:
            self.save_mapping()
            logger.info(f"🎉 Successfully added {successful_updates} new sector mappings")
            
            # Display summary
            self.display_update_summary(updates_made)
        else:
            logger.info("ℹ️ No new sector mappings were added")
        
        return updates_made
    
    def display_update_summary(self, updates_made: Dict):
        """Display a summary of the updates made"""
        print("\n" + "="*60)
        print("🔄 SECTOR MAPPING UPDATE SUMMARY")
        print("="*60)
        
        # Group by major sector
        sector_groups = {}
        for detailed, major in updates_made.items():
            if major not in sector_groups:
                sector_groups[major] = []
            sector_groups[major].append(detailed)
        
        for sector, detailed_list in sector_groups.items():
            print(f"\n📊 {sector}: {len(detailed_list)} new mappings")
            for detailed in detailed_list[:5]:  # Show first 5
                print(f"   • {detailed}")
            if len(detailed_list) > 5:
                print(f"   ... and {len(detailed_list) - 5} more")
        
        print(f"\n📈 Total new mappings: {len(updates_made)}")
        print(f"💾 Updated file: {self.mapping_file}")
        print("="*60)

# Main execution function
async def main():
    """Main function to run the sector mapping updater"""
    
    # Use the same directories as your main trading code
    MAIN_DIR = "c:/Users/kyung/StockScanner/MAIN"
    RANKING_DIR = os.path.join(MAIN_DIR, "ranking_results", "sectors")
    MAPPING_FILE = os.path.join(MAIN_DIR, "sector_mapping.json")
    
    # Check if directories exist
    if not os.path.exists(RANKING_DIR):
        print(f"❌ Error: Ranking directory not found: {RANKING_DIR}")
        return
    
    if not os.path.exists(MAPPING_FILE):
        print(f"❌ Error: Sector mapping file not found: {MAPPING_FILE}")
        return
    
    print("🔍 Sector Mapping Updater")
    print("="*50)
    print(f"Ranking Directory: {RANKING_DIR}")
    print(f"Mapping File: {MAPPING_FILE}")
    print("="*50)
    
    # Initialize and run the updater
    updater = SectorMappingUpdater(RANKING_DIR, MAPPING_FILE)
    updates = await updater.update_sector_mapping()
    
    if updates:
        print(f"\n✅ Sector mapping update completed successfully!")
        print(f"📊 Added {len(updates)} new sector mappings")
    else:
        print(f"\nℹ️ No updates were needed - all sectors are properly mapped!")

# Command line execution
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Update sector mapping to eliminate "Other" category')
    parser.add_argument('--ranking-dir', help='Directory containing sector ranking CSV files')
    parser.add_argument('--mapping-file', help='Path to sector_mapping.json file')
    
    args = parser.parse_args()
    
    # Use provided paths or defaults
    ranking_dir = args.ranking_dir or "c:/Users/kyung/StockScanner/MAIN/ranking_results/sectors"
    mapping_file = args.mapping_file or "c:/Users/kyung/StockScanner/MAIN/sector_mapping.json"
    
    # Run the updater
    asyncio.run(main())