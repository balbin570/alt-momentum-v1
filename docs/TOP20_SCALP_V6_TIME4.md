# TOP20_SCALP_V6 — küçük zaman çıkışı denemesi

16 Kasım 2026. Kullanıcı C değişikliğinden sonra "t 20 yede dokunsana" diyerek Top20 için de küçük paper değişikliği istedi. Hedef daha önce doğrulanmış Mucahit's workspace / mucahit bağlantısındaki alt-momentum-v1 servisidir. C'nin devredeki C_ENTRY_BREAKOUT050_V1 değişikliği korunur.

## Tek parametre

Yeni Top20 girişlerinin azami bekleme süresi 5 -> 4 dakika. Yeni kohort TOP20_SCALP_V6. TP +%0.40, SL -%0.25, erken momentum kaybı çıkışı, giriş filtreleri, 25 saniye onayı, spread/chase sınırları, pozisyon sayısı/büyüklüğü ve %0.15 maliyet varsayımı aynı. Yeni işlem time_exit_minutes=4 taşır. Mevcut V5 açık kayıtları 5 dakika ve erken momentum çıkışını; daha eski V1-V4 kayıtları önceki 10 dakika davranışını korur. V5 ACTIVE_MODELS içinde tutulur; eski işlemler takipsiz bırakılmaz. Durum endpoint'i yeni V6 stats ve ayrı baseline_v5_stats döndürür. Giriş mesajı sürümü dinamik olarak V6 gösterir.

## Hipotez ve sınırı

Dağıtım öncesi V5 172 kapanış: MOMENTUM_LOST n86 ortalama -%0.295219; STOP_MONITORED n19 -%0.449154; TP n20 +%0.3204455; TIME5 n47 -%0.071379. TIME5 alt grubu negatif olsa da 4 dakikada çıkmanın daha iyi olduğu bilinmiyor; tam fiyat yolu yok ve sonuç nedenine göre seçilmiş alt grup nedensellik kanıtı değildir. Alternatif geçmiş TIME4 getirisi üretilmedi. Bu kullanıcı tarafından onaylanmış ileri sanal denemedir; daha erken çıkış bazı kazananları da kaçırabilir. İkinci bir Top20 parametresi bu döngüde değiştirilmedi.

## Yazılım doğrulaması

6 yeni bütünleşme testi geçti: 239/240 saniye V6 sınırı; V5'in 5 dakikasının korunması; V4'ün 10 dakikası; V5 ve V6 erken momentum çıkışı; TP/SL önceliği; yeni girişte V6/time_exit_minutes ve aynı TP/SL/notional; status kohort ayrımı. py_compile geçti. Tüm test paketi 42 sonuç, aynı önceden var olan 3 shadow uyumsuzluğu + eksik select_leaders import hatası. Önceki C yamasından 5 hedefli test de başarılı; yeni regresyon gözlenmedi. Tüm paket yeşil olarak sunulmaz. İlk yeni test çalışmasındaki saat mock'u event loop saatini etkiledi; yalnız test saat bağımlılığı yalıtılıp 6 test tekrar başarıyla çalıştırıldı.

## İzleme ve geri alma

/top20-scalp-status.model_name TOP20_SCALP_V6, time_exit_minutes=4; stats yalnız yeni kohort, baseline_v5_stats eski kohort. /paper-records variant ile ayrı okunmalı. C yeni giriş sürümü aynen izlenmeli; eski V5 aday spread filtresi yeni V6 timeout denemesiyle havuzlanmamalı. Yeni sürümde veri yoksa başarı iddiası yok; yeterli yeni kapanış olmadan yeni parametre değiştirme. 8 saatlik otomasyon yeni kohortu izleyecek.

Önceki canlı kaynak commit 2035fd7326e6eb75c25fb02b91403892149f00bd; bu yama yalnız top20_scalp.py, yeni test ve bu dokümanı değiştirir. Kullanıcı daha erken çalışma alanı seçmiştir, tekrar izin gerekmez. AutoDeploy=yes; main güncellemesi tek deploy başlatır. Yayına geçiş Render live ve endpoint/checkpoint ile ayrıca doğrulanmalı. Geri alma sırasında V6 açık işlemleri ACTIVE_MODELS dışına çıkarılmamalı veya süreleri uzatılmamalı; sonraki girişler yeni sürüm etiketiyle 5 dakikaya döndürülmeli. İşlem geçmişi sıfırlanmaz, eski kapanmış netler değiştirilmez. Gerçek emir, ücretli kaynak, büyüyen risk limiti veya bağımsız Telegram test mesajı yok.

