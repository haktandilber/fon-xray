import asyncio
import aiohttp
import json
import os
import re
from datetime import datetime, timedelta

def parse_tarih(val):
    if isinstance(val, str) and "Date" in val:
        match = re.search(r'\d+', val)
        return float(match.group()) if match else 0
    try:
        return float(val)
    except:
        return 0

async def fon_dagilimini_getir(session, fon_kodu, fon_tipi, bas_tarih, bit_tarih, semaphore):
    async with semaphore:
        # IP ban yememek için istekler arasına 0.5 saniye koyduk
        await asyncio.sleep(0.5) 
        
        url = "https://www.tefas.gov.tr/api/DB/BindHistoryAllocation"
        payload = {
            "fontip": fon_tipi, "sfontur": "", "fongrup": "",
            "fonkod": fon_kodu, "bastarih": bas_tarih, "bittarih": bit_tarih
        }
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        try:
            async with session.post(url, data=payload, headers=headers, timeout=20) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get("data") and len(data["data"]) > 0:
                        en_guncel = max(data["data"], key=lambda x: parse_tarih(x.get("TARIH", 0)))
                        
                        tarih_ms = parse_tarih(en_guncel.get("TARIH", 0))
                        tarih_str = datetime.fromtimestamp(tarih_ms / 1000.0).strftime('%Y-%m-%d')
                        
                        sonuc = {
                            "fonKodu": fon_kodu,
                            "fonTipi": fon_tipi,
                            "tarih": tarih_str,
                            "dagilim": {}
                        }
                        #'ÖKSYD', 'ÖSDB'
                        haric = ['TARIH', 'FONKODU', 'FONUNVAN','BilFiyat']
                        print(en_guncel.items())
                        for k, v in en_guncel.items():
                            if k not in haric and v is not None and float(v) > 0:
                                sonuc["dagilim"][k] = float(v)
                        
                        # Dosyaya yazmıyoruz, veriyi geri döndürüyoruz
                        return sonuc
        except Exception as e:
            print(f"Hata: {e} - Fon Kodu: {fon_kodu}")
            
        return None

async def main():
    bugun = datetime.now()
    bas_tarih = (bugun - timedelta(days=30)).strftime("%d.%m.%Y")
    bit_tarih = bugun.strftime("%d.%m.%Y")
    
    try:
        with open('fundlist.json', 'r', encoding='utf-8') as f:
            veri = json.load(f)
            tum_fonlar = veri.get('funds', [])
    except FileNotFoundError:
        print("Hata: fundlist.json dosyası bulunamadı!")
        return

    # Test için yine ilk 50 fonu alıyoruz
    test_fonlari = tum_fonlar
    print(f"Test modu aktif: {len(test_fonlari)} fon için istek atılıyor...\n")
    
    # Çok güvenli eşzamanlılık sınırı (Aynı anda max 2 istek)
    semaphore = asyncio.Semaphore(2)
    connector = aiohttp.TCPConnector(limit_per_host=2)
    
    # Tüm verileri toplayacağımız ana sözlük
    tum_dagilimlar = {}
    
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fon_dagilimini_getir(
                session, fon['fundCode'], fon['fundType'], bas_tarih, bit_tarih, semaphore
            )
            for fon in test_fonlari
        ]
        
        # Görevlerin bitmesini bekle ve sonuçları al
        sonuclar = await asyncio.gather(*tasks)
        
        # Gelen sonuçları ana sözlüğe ekle
        for sonuc in sonuclar:
            if sonuc: # Eğer başarılı bir veri döndüyse
                fon_kodu = sonuc.pop("fonKodu") # Kodu anahtar (key) yapmak için içinden alıyoruz
                tum_dagilimlar[fon_kodu] = sonuc

    # Döngü bittikten sonra hepsini TEK BİR DOSYAYA yaz
    dosya_adi = "funds_allocation.json"
    with open(dosya_adi, "w", encoding="utf-8") as f:
        json.dump(tum_dagilimlar, f, ensure_ascii=False, indent=2)

    print(f"\nİşlem tamamlandı! {len(tum_dagilimlar)} fonun verisi '{dosya_adi}' dosyasına yazıldı.")

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())