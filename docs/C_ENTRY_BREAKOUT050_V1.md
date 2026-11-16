# C giriş denemesi — 16 Kasım 2026

Kullanıcı 16 Kasım'da "ok. yavaş yavaş azar azar değiştirelim" diyerek küçük paper değişikliklerinin denenmesini onayladı. Bu, önceki otomasyonun uygulama öncesi 7 gün/50 işlem şartını bu küçük ileri paper denemesi için kaldırır; kârlılığın doğrulandığı anlamına gelmez.

## Tek davranış değişikliği

Yalnız yeni C girişlerinin breakout üst sınırı %1.50 yerine %0.50. Alt sınır %0.20, momentum, hacim, z, spread, chase, pozisyon sayısı/büyüklüğü, stop, kâr alma ve zaman çıkışı aynıdır. Top20, Hızlı Momentum ve kapalı C shadow değiştirilmedi. Yeni kayıtlar c_entry_version=C_ENTRY_BREAKOUT050_V1 ve c_entry_breakout_max_pct=0.50 taşır. c_rule_version=C_V2_PROTECT çıkış sürümüdür ve aynen kalır. Eski açık kayıtların çıkış davranışı veya geçmiş kayıtlar değiştirilmez.

## Kanıtın sınırı

Son incelemede kesim sonrası C referansı 18 kapanışta ortalama -%1.3226; breakout<=%0.50 alt grubu 10 kapanışta -%0.8878, PF 0.2248. İlave %0.10 puan maliyette ortalama -%0.9878. Bu alt grup hâlâ zararlıdır; filtre karşılaştırması portföy getirisi veya neden-sonuç kanıtı değildir. Değişiklik, kullanıcının istediği kontrollü sanal denemedir. Tarihsel temel sürüm eşzamanlı kontrol değildir.

## Doğrulama

Beş yeni bütünleşme testi başarılı: yeni giriş etiketi ve aynı çıkış sürümü, sınır üstü girişin reddi, mevcut alt sınırın korunması, açık V2 ve yeni etiketli işlemin aynı başa baş/çıkış davranışı, salt okunur kohort ayrımı. py_compile başarılı.

Değişiklik öncesi tüm testlerde 31 sonuç: 3 başarısızlık + 1 import hatası; sonrası 36 sonuç: aynı 3 başarısızlık + aynı 1 import hatası, 5 yeni test başarılı. Mevcut uyumsuzluklar: test_top20_scalp eksik select_leaders fonksiyonunu import ediyor; üç eski shadow testi artık emekli C_SHADOW_MOM1'in açılmasını/eski status kapsamını bekliyor. Bunlar bu yama tarafından oluşturulmadı ve bu yama kapsamında stratejiyi genişleterek düzeltilmedi. Tüm test paketi başarılı olarak sunulmaz.

## Takip ve geri alma

/c-entry-experiment salt okunur endpoint'i yeni sürümü, giriş aralığını, baseline_v2 ve C_ENTRY_BREAKOUT050_V1 sonuçlarını ayrı verir. /c-loss-analysis içine active_c_entry_version ve c_entry_cohorts eklenir. Eski c_v2_stats çıkış sürümü toplamıdır; yeni giriş denemesini tek başına temsil etmez. /paper-records üzerinden yeni/eskileri c_entry_version ile ayır.

İlk dağıtım doğrulandıktan sonra her 8 saatte net sonuç, PF, kazanma oranı, düşüş, çıkış nedenleri, sembol/zaman yoğunluğu ve kayıt tazeliği izlenir. Sinyal yoksa filtreyi gevşetme; yeterli yeni işlem olmadan ikinci parametre değiştirme. Kötüleşme değerlendirmesi değişen piyasa dönemi ve örneklem belirsizliğini içermeli. Yeni bir kural değişikliği ayrı sürüm olmalı.

Önceki main commit ce0ff46af4c05ba03eccd1efa5b79d2dfe5be3a1. Geri alma, yalnız yeni giriş üst sınırını %1.50'ye döndürüp ayrı giriş sürümü ile kaydetmelidir; geçmişi sıfırlamamalı. Yeni etiketli açık işlemler c_rule_version=C_V2_PROTECT ile mevcut çıkış kurallarını korur. Dağıtım/restart sırasında mevcut Supabase checkpoint korunmalıdır.

## Dağıtım durumu

Bu kayıt oluşturulurken üretime dağıtılmadı; yama ayrı dalda hazır. Render araçları her iki bağlantıda da "no workspace selected" döndürdü ve list_workspaces sonrası kullanıcıdan çalışma alanı seçilmesini açıkça istedi. Seçenekler: Mucahit's workspace (mucahit bağlantısı) veya Balbin's workspace (Primary bağlantısı). Hedef seçilmeden main'e yazılıp olası otomatik deploy tetiklenmedi. Yeni ücretli kaynak, gerçek emir veya ek mesaj gönderilmedi.

