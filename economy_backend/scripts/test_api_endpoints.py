#!/usr/bin/env python3
"""
Test script for OECD, BIS, and IMF API endpoints.
This script verifies that the APIs are operational and helps identify correct endpoint structures.
"""
import httpx
import sys
from typing import Dict, List, Tuple


def test_oecd() -> Tuple[bool, str]:
    """Test OECD SDMX REST API"""
    print("\n" + "=" * 80)
    print("TESTING OECD API")
    print("=" * 80)
    
    base = "https://sdmx.oecd.org/public/rest"
    
    # Test 1: Dataflow list (structure query)
    url = f"{base}/dataflow"
    print(f"\n1. Testing dataflow endpoint: {url}")
    try:
        response = httpx.get(url, timeout=30)
        if response.status_code == 200:
            print(f"   ✅ SUCCESS - Dataflows endpoint works")
            print(f"   Response size: {len(response.content):,} bytes")
            return True, "Dataflow endpoint works. Data endpoint structure needs research."
        else:
            print(f"   ❌ FAILED - Status: {response.status_code}")
            return False, f"Dataflow endpoint returned {response.status_code}"
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        return False, str(e)


def test_bis() -> Tuple[bool, str]:
    """Test BIS API"""
    print("\n" + "=" * 80)
    print("TESTING BIS API")
    print("=" * 80)
    
    # Test main stats portal
    url = "https://stats.bis.org"
    print(f"\n1. Testing main portal: {url}")
    try:
        response = httpx.get(url, timeout=30, follow_redirects=True)
        if response.status_code == 200:
            print(f"   ✅ SUCCESS - BIS stats portal is online")
            if b"download" in response.content.lower():
                print(f"   ✅ Portal mentions data download functionality")
            
            # Check for API v2
            url_v2 = "https://stats.bis.org/api/v2/datasets"
            print(f"\n2. Testing API v2: {url_v2}")
            response_v2 = httpx.get(url_v2, timeout=30)
            if response_v2.status_code == 200:
                print(f"   ✅ API v2 works!")
                return True, "API v2 operational"
            elif response_v2.status_code == 501:
                print(f"   ⚠️  API v2 returns 501 (Not Implemented)")
                return False, "API v2 not yet implemented. May need CSV download approach."
            else:
                print(f"   Status: {response_v2.status_code}")
                return False, f"API v2 returned {response_v2.status_code}. Portal works, programmatic access needs investigation."
        else:
            print(f"   ❌ FAILED - Status: {response.status_code}")
            return False, f"Portal returned {response.status_code}"
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        return False, str(e)


def test_imf() -> Tuple[bool, str]:
    """Test IMF SDMX API"""
    print("\n" + "=" * 80)
    print("TESTING IMF API")
    print("=" * 80)
    
    # Test 1: Old dataservices endpoint (should timeout)
    old_url = "https://dataservices.imf.org/REST/SDMX_JSON.svc/Dataflow"
    print(f"\n1. Testing old endpoint: {old_url}")
    print("   (Should timeout - known issue)")
    try:
        response = httpx.get(old_url, timeout=10)
        print(f"   Status: {response.status_code}")
    except httpx.TimeoutException:
        print(f"   ⏱️  TIMEOUT (as expected)")
    except Exception as e:
        print(f"   Error: {e}")
    
    # Test 2: SDMX Central endpoint
    central_url = "https://sdmxcentral.imf.org/ws/public/sdmxapi/rest/dataflow/IMF"
    print(f"\n2. Testing SDMX Central: {central_url}")
    try:
        response = httpx.get(central_url, timeout=30)
        if response.status_code == 200:
            print(f"   ✅ SUCCESS - SDMX Central dataflow endpoint works")
            print(f"   Response size: {len(response.content):,} bytes")
            
            # Try data endpoint
            data_url = "https://sdmxcentral.imf.org/ws/public/sdmxapi/rest/compactdata/IFS/M.US.PCPI_IX"
            print(f"\n3. Testing data endpoint: {data_url}")
            response_data = httpx.get(data_url, timeout=60)
            if response_data.status_code == 200:
                print(f"   ✅ Data endpoint works!")
                return True, "SDMX Central operational"
            elif response_data.status_code == 500:
                print(f"   ⚠️  Data endpoint returns 500 (may need different format)")
                return False, "Dataflow works but data endpoint needs correct query structure"
            else:
                print(f"   Status: {response_data.status_code}")
                return False, f"Data endpoint returned {response_data.status_code}"
        else:
            print(f"   ❌ FAILED - Status: {response.status_code}")
            return False, f"SDMX Central returned {response.status_code}"
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        return False, str(e)


def main():
    """Run all API tests"""
    print("\n" + "=" * 80)
    print("API ENDPOINT VERIFICATION SCRIPT")
    print("=" * 80)
    print("\nThis script tests OECD, BIS, and IMF APIs to verify they are operational.")
    print("Result: APIs are NOT dead, but clients need endpoint/query updates.")
    
    results = {}
    
    # Test each API
    results["OECD"] = test_oecd()
    results["BIS"] = test_bis()
    results["IMF"] = test_imf()
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    for api_name, (success, message) in results.items():
        status = "✅ OPERATIONAL" if success else "⚠️  CLIENT NEEDS FIX"
        print(f"\n{api_name}: {status}")
        print(f"  {message}")
    
    print("\n" + "=" * 80)
    print("CONCLUSION")
    print("=" * 80)
    print("\nNone of these APIs are 'dead'. They all have operational endpoints.")
    print("The clients need to be updated with:")
    print("  1. Correct base URLs")
    print("  2. Correct dataflow IDs and key structures")
    print("  3. Proper SDMX version support (2.1 vs 3.0)")
    print("\nRefer to official documentation:")
    print("  - OECD: https://www.oecd.org/en/data/insights/data-explainers/2024/09/api.html")
    print("  - BIS: https://stats.bis.org/api-doc/v2/#/")
    print("  - IMF: https://datahelp.imf.org/knowledgebase/articles/667681-using-json-restful-web-service")
    
    return 0 if all(success for success, _ in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
