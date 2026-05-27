#!/usr/bin/env python3
"""Test script for the fixed Autoshopify code"""

import asyncio
import sys
from Autoshopify import fetch_products, process_card

async def test_sites():
    """Test the fixed code with the two sites"""
    
    sites = [
        "https://www.seedsnow.com/",
        "https://1836-country-store.myshopify.com"
    ]
    
    # Test credit card (fake for testing)
    cc = "4111111111111111"
    month = "12"
    year = "2025"
    cvv = "123"
    
    print("=" * 60)
    print("Testing fixed Autoshopify code")
    print("=" * 60)
    
    for site in sites:
        print(f"\n\n{'='*60}")
        print(f"Testing: {site}")
        print(f"{'='*60}")
        
        try:
            # Test 1: Fetch products
            print(f"\n[1] Fetching products from {site}...")
            result = await fetch_products(site)
            
            if isinstance(result, dict) and result.get('variant_id'):
                print(f"✓ Products fetched successfully")
                print(f"  - Cheapest product: ${result['price']}")
                print(f"  - Variant ID: {result['variant_id']}")
                
                # Test 2: Process card with the fetched variant
                print(f"\n[2] Processing payment...")
                success, message, gateway, total, currency = await process_card(
                    cc=cc,
                    mes=month,
                    ano=year,
                    cvv=cvv,
                    site_url=site,
                    variant_id=result['variant_id']
                )
                
                if success:
                    print(f"✓ Payment processed!")
                    print(f"  - Gateway: {gateway}")
                    print(f"  - Total: {currency} {total}")
                else:
                    print(f"✗ Payment failed: {message}")
                    
            else:
                print(f"✗ Failed to fetch products: {result}")
                
        except Exception as e:
            print(f"✗ Error: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print(f"\n\n{'='*60}")
    print("Testing complete")
    print(f"{'='*60}")

if __name__ == "__main__":
    asyncio.run(test_sites())
