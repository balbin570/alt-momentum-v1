# ALT-MOMENTUM-V1 — Devralma incelemesi

İncelenen başlangıç commit'i: `77cca71dc0410ee24c09a7e70172a5b7f2ebbd3f`.
Kontrol tarihi: 11 Kasım 2026. Bu belge canlı performans raporu değildir.

## Doğrulananlar

- Üretim kodundaki FastAPI uygulaması `main.py`. Piyasa verileri `data-api.binance.vision` üzerinden yalnız GET istekleriyle alınıyor. Binance emir yolu veya imzalı emir istemcisi yok. `orders=false`, `trading=false`.
- Başlangıç sürümünde yeni giriş modeli yalnız `C_TREND`; beş çıkış varyasyonu aynı girişin gözlem kopyalarıydı. Kullanıcının sonraki açık talebiyle bu dalda yalnız **C adlı model** yeni paper giriş açacak. C_TREND dahil diğer modellerin yeni girişleri donduruldu; mevcut açık işlemlerin çıkış takibi korunuyor.
- Giriş: 60 dakika momentum [3,7], hacim [1.5,3.5), z<3.5, breakout [0.2,1.5], spread<=0.15%, chase<=0.5%; tazelik, tekrar ve kapasite kontrolleri de var. C_TREND için ayrıca 0.12% spread sınırı uygulanmıyor. Bu C_TREND kuralları değiştirilmedi; modelin yeni girişleri donduruldu. Baseline **C** için mevcut kurallar: momentum [2.5,8], hacim [1.2,3.5), z<3.5, breakout [0.2,1.5], spread<=0.12%, chase<=0.5%. C giriş/çıkış eşikleri değiştirilmedi.
- C_TREND çıkışında gözlenen +3% seviyesinde yarım kâr kaydı, başlangıç -3% stop ve 120 dakika çıkışı var; maliyet 0.15 yüzde puan. Stoplar 5 dakikalık gözlemlerle çalışıyor.
- Yeni giriş sözlüğü BTC 60dk getirisi, pozitif altcoin oranı, momentum, hacim, spread ve zamanı içeriyor; kapanınca net getiri ekleniyor. Checkpoint bu alanları saklıyor. Eski işlemlere veri eklenmiyor.
- Checkpoint aynı klasörde geçici dosya, flush/fsync ve atomik rename ile yazılıyor. Varsayılan `/tmp/v90_state.json` kalıcı disk garantisi değildir. Gerçek Render ayarı ve disk eşleşmesi henüz doğrulanamadı.
- Bildirim başarısızlığı paper giriş/çıkışını geri almıyor. Telegram hata metni token içeren URL'yi loglayabildiği için bu dalda yalnız hata sınıfı ve HTTP durum kodu saklanıyor.

## Canlı erişim engeli

Servisin `/v90-status` ve `/openapi.json` istekleri HTTP 503 döndürdü; yanıt başlığı “Service Suspended”, gövdesi “This service has been suspended by its owner.” diyor. Bu gözlem tek başına askıya alınma nedenini veya tarihini göstermiyor.

Render bağlantısı yalnız Balbin's workspace gösterdi. Kullanıcı bu çalışma alanını reddetti ve Mucahit's workspace bağlantısını önerdi. Balbin alanında servis/log sorgusu veya değişiklik yapılmadı. Sonraki workspace sorgusu bağlantı kimliği eksik (`link_id`) hatası verdi; yeni bağlantı henüz doğrulanamadı. Doğru bağlantı gelene kadar son deploy, hata kayıtları, kalıcı disk, gerçek checkpoint ve son sinyal zamanı doğrulanamaz. Verilen 141 işlem raporu yeni bir canlı rapor olarak sunulmadı.

## Bulgular ve bu daldaki düzeltmeler

1. Hacim kontrolü momentum reddini üzerine yazıyor. Bu nedenle 81/19 dağılımından her filtrenin bağımsız etkisi çıkarılamaz. Eski karar akışı korunuyor; yeni `/c-entry-diagnostics` bağımsız başarısızlık sayıları veriyor.
2. Dokuz momentum/hacim kombinasyonunun aday sayıları ve sabit z/breakout filtrelerinden sonra kalan sayıları mevcut tarama verilerinden hesaplanıyor. Son tarama ve birikimli toplamlar checkpoint'e ekleniyor. Bağımsız ret sayımları artık seçilen baseline C eşiklerini kullanıyor. İlave piyasa isteği ve alternatif pozisyon açılışı yok. Spread/chase/kapasite kontrolü alternatiflere uygulanmadığından bunlar tamamlanmış giriş sinyali veya performans ölçümü değildir.
3. Mevcut kod 1.200 toplam kayıt aşılınca en eski kapanmış paper kayıtlarını siliyor. Bu dal bu otomatik silmeyi kaldırıyor. RAM/disk büyümesi gerçek Render planında değerlendirilmeli; uzun vadede silmeden arşivleme tasarlanmalı. Önceden silinen veri geri üretilemez.
4. Checkpoint okunamadığında sonraki tarama boş durumla dosyanın üzerine yazabiliyor. Bu dal restore hatası sonrası checkpoint yazmayı bloke ediyor. Hasarlı dosyayı otomatik onarmıyor; servis hâlâ bellekte çalışabilir. Operasyonel hata giderilmeden deploy yapılmamalı.
5. GitHub zamanlanmış workflow `/v22-scan-now` çağırıyor; mevcut uygulamada böyle bir route yok. Dahili tarayıcı startup ile çalışıyor. Workflow veya aktif strateji bu dalda değiştirilmedi.

## C zarar gözlemi

`/c-loss-analysis` yalnız C kayıtlarını inceler. Net kayıplar çıkış nedenine ve kaydedilmiş giriş momentumu, hacim, z, breakout, spread, chase alanlarına göre gruplandırılır. Eksik tarihsel alanlar eksik olarak kalır.

Yeni C girişlerinde beş hipotezin kararı giriş anında saklanır: BTC getirisi negatif; piyasa genişliği %50 altında; chase %0.25 üstünde; spread %0.10 üstünde; z 2.5 üstünde. Bunlar C'nin girişlerini engellemez. İleri örneklem oluştuğunda her hipotez için kaç kaybı önleyeceği, kaç kazananı kaçıracağı, tutulacak/atlanacak grupların işlem sayısı, ortalama net getirisi, kazanma oranı ve Profit Factor raporlanır. Bilinmeyen veri güvenli veya riskli sayılmaz. Eski işlemlere bu kararlar geriye dönük eklenmez.

C_TREND'nin 141 işlem sonucu C'ye atfedilmedi. Canlı C geçmişi henüz alınamadığı için zararın kanıtlanmış sebebi veya kâr artışı iddiası yoktur. Otomatik risk engelleme yeterli forward karşılaştırma olmadan etkinleştirilmedi.

## Doğrulama

`python -m unittest discover -s tests -v`: 14 test başarılı. Testler geçici checkpoint dosyaları ve sahte piyasa/Telegram yanıtları kullanıyor; gerçek bildirim veya emir göndermiyor.

Kontroller: eski checkpoint uyumluluğu, yeni alanların saklanması, 1.205 kaydın korunması, bozuk checkpoint'in üzerine yazılmaması, token gizliliği, bağımsız filtre sayımı ve sınırlar, tanı endpoint'inin salt okunurluğu, eski işlemlere rejim atanmaması, yalnız C girişlerinin açılması, giriş anı hipotezlerinin kaydı, önlenen kayıp/kaçırılan kazanç hesabı ve dondurulmuş çıkış varyasyonlarının mevcut pozisyonları kapatmaya devam etmesi.

Başlangıç sürümüyle AST karşılaştırmasında C/C_TREND ve varyasyon çıkış fonksiyonları ile piyasa veri fonksiyonları aynı bulundu. C giriş filtreleri ve pozisyon oluşturma kuralları aynı; aktif model seçimi kullanıcı talebiyle C oldu, girişe yalnız gözlem metadatası ekleniyor. `py_compile` ve `git diff --check` başarılı.

## İleri araştırma hipotezleri

- BTC düşüşü ve düşük piyasa genişliği, C sonuçlarıyla ilişkili olabilir. Yalnız giriş anı verisi bulunan yeni C işlemleriyle incelenecek. Eski C_TREND rejim endpoint'i ayrı kalır.
- Düşük sinyal sayısı dar momentum/hacim kesişiminden kaynaklanabilir. Bağımsız ret ve alternatif aralık sayımları bunu sınar; daha fazla aday daha yüksek kâr demek değildir.
- Aynı anda açılan işlemler ilişkili olabilir. Yeterli örneklemde günlük/zaman bloklu bootstrap ile ortalama getiri ve Profit Factor güven aralıkları; kazanma oranında Wilson aralığı düşünülebilir. Mevcut veriyle yeni bir istatistiksel sonuç iddia edilmiyor.

V97 arşiv dosyaları aktif uygulamaya dahil edilmedi; V13_FROZEN_FORWARD_PAPER bu araştırmaya katılmadı. Canlı checkpoint'e erişilmedi, deploy/restart/resume yapılmadı.
