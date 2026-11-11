# C forward paper — işletim ve araştırma kaydı

11 Kasım 2026. Gerçek emirler kapalıdır: `orders=false`, `trading=false`.

## Son davranış

Kullanıcı baseline **C adlı modeli** seçti. Yalnız C yeni paper işlem açar; C_TREND ve diğer tüm modellerin yeni girişleri dondurulmuştur. Mevcut eski açık işlemlerin kendi çıkış kuralları korunur. C'nin momentum [2.5,8], hacim [1.2,3.5), z<3.5, breakout [0.2,1.5], spread<=0.12% ve chase<=0.5% giriş eşikleri aynı kalır; stop -3%, gözlenen +3% yarım kâr ve 120 dakika çıkış seviyeleri değişmez.

Yeni C işlemlerinin çıkışları yaklaşık 60 saniyede bir kontrol edilir; giriş taraması 300 saniyedir. Tarama, ağ gecikmesi ve Telegram isteği kontrol aralığını uzatabilir; gap ve stop aşımı hâlâ mümkündür. Hızlı takip yalnız `c_forward_version=C_FORWARD_SAFETY_V1` ile açılan yeni C kayıtlarına uygulanır. Eski C ve C_TREND sonuçları yeni kohortla birleştirilmez.

## Gerçek geçmiş ve veri kurtarma

Silinmeden önce `/v914-closed-trades` üzerinden 1.184 kapanmış işlemin tamamı dışa aktarıldı. Önceki checkpoint toplam 1.200 kayıt içeriyordu. Render olay kaydı eski diskin 11 Kasım 2026 17:37:20 UTC'de silindiğini gösterdi; kullanıcı disk/servis ayarını değiştirdiğini ve eski diskin silindiğini doğruladı. Sonraki servis `/var/data` için PermissionError ve sıfır yüklenmiş kayıt bildirdi.

Kullanıcının ücretsiz Supabase tercihiyle mevcut bağlı projede **ayrı** `alt_momentum_v1_checkpoint` ve `alt_momentum_v1_recovery_backup` tabloları oluşturuldu. Başka model tabloları değiştirilmedi. Yeni proje, ücretli kaynak veya plan yükseltmesi oluşturulmadı.

1.184 özgün kapanmış kayıt Supabase'e aktarıldı, okunup SHA-256 ile doğrulandı: `c8e1471c961054e14b2073f72287bfe53af0a7bfc658ff640b0af0b0e006ac6c`. Geri alınamayan 16 kayıt açıkça `partial_recovery=true` olarak raporlanır. Bu 16 kaydın içeriği veya eski Telegram sayaçları uydurulmadı. Sinyal sayacı, disk silinmeden önce doğrulanan 441 değerinden başlar; kaybolan ara işlemler hakkında iddia içermez.

## Kalıcılık ve erişim

Render diskine ihtiyaç yoktur. Sıkıştırılmış checkpoint yalnız bu projeye ait Edge Function üzerinden yazılır. Yeni, yüksek entropili erişim sırrı yalnız Render ortam ayarındadır. İşlev kodunda sadece SHA-256 doğrulayıcısı bulunur. Supabase yönetici anahtarı yalnız Supabase'in kendi sunucu ortamında kullanılır; Render'a aktarılmaz. Telegram token'ı değiştirilmez ve hata metninde URL loglanmaz.

Tablolarda RLS açık; anon ve authenticated rollerinin erişimleri kaldırılmıştır. Yalnız sunucu rolüne gerekli izinler verilmiştir. Yeni tablolarda politika bulunmaması bilinçlidir: halka açık erişim yoktur. [RLS danışman açıklaması](https://supabase.com/docs/guides/database/database-linter?lint=0008_rls_enabled_no_policy). Projedeki önceden var olan `rls_auto_enable()` danışman uyarıları bu değişiklik kapsamında değiştirilmedi: [anon erişim danışmanı](https://supabase.com/docs/guides/database/database-linter?lint=0028_anon_security_definer_function_executable).

Yazım beklenen revision ile atomik karşılaştırılır; eski süreç güncel kaydın üzerine yazamaz. Geçmiş işlemlerin çıkarılması veya kapanmış net sonuçlarının değiştirilmesi sunucuda reddedilir. Başlangıçta Supabase okunamaz/yazılamazsa paper tarayıcı boş geçmişle başlatılmaz. Sonraki yazma hatalarında yeni girişler engellenir, mevcut işlemlerin çıkış takibi ve yazma denemeleri sürer. Otomatik 1.200 kayıt silmesi kaldırılmıştır. Uzun vadede RAM, veritabanı ve ücretsiz kullanım kotaları izlenmelidir; ücretli yükseltme otomatik yapılmaz.

## Araştırma

`/c-loss-analysis` yalnız C'yi inceler; eski C ve yeni bir dakikalık takip kohortunu ayrı verir. Çıkış nedenleri ve kaydedilmiş giriş özellikleriyle gruplama yapar. BTC negatif, genişlik %50 altı, chase %0.25 üstü, spread %0.10 üstü ve z<=2.5 hipotezleri yeni giriş anında yalnız gözlem olarak saklanır. Otomatik giriş engellemesi bu hipotezlere bağlanmamıştır.

134 tarihsel C işleminde ortalama net -%0.0718, PF 0.9403; 29 stop çıkışının toplamı -101.1391 yüzde puan, 105 zaman çıkışının toplamı +91.5203 yüzde puandır. Bu, giriş anı nedensellik kanıtı değildir. En kötü C kaydı -%5.56601'dir; beş dakikalık gözlem stop aşımına katkıda bulunabilir, ancak tam tick geçmişi yoktur.

z>2.5 alt grubunda 45 işlemde ortalama +%0.3811 görülmüştür; kronolojik son 21 işlemde +%0.0546'ya geriler. Dolayısıyla bu filtre doğrulanmış kârlı bir sonuç sayılmaz. Yeni örneklemde kaç zarar önlendiği ve kaç kazanan kaçırıldığı birlikte ölçülür. Önlenen getiriler portföy getirisi değildir; kapasite ve alternatif giriş etkileri hesaba katılmaz. Hipotezler ilişkili olduğundan toplamları toplanmaz.

`/c-entry-diagnostics` dokuz momentum/hacim aralığının aday sayımını yapar. Ek piyasa isteği veya alternatif pozisyon oluşturmaz; spread/chase/kapasite öncesi sayımlardır. `/paper-records` tüm durumları sayfalı, salt okunur verir. Eski rejim endpoint'i C_TREND kapsamını korur. Tarihsel işlemlere piyasa verisi veya karar eklenmez. V97 arşivi ve V13 bu araştırmaya katılmaz.

## Testler ve Telegram

`python -m unittest discover -s tests -v`: 28 test başarılı. `py_compile` ve `git diff --check` başarılı. Gerçek Supabase testlerinde yetkisiz erişim 401, eski revision 409, geçmiş silme/sonuç değiştirme 422 ile reddedilir. İlk yazım ve geri okuma 1.184 kaydın tamamını doğrulamıştır.

Telegram giriş/çıkış şablonları korunur. Kullanıcının talebiyle dağıtım sonrası `/v90-telegram-test` bir kez çağrılıp teslim sonucu kontrol edilir. Sinyal yoksa zorla işlem oluşturulmaz. Kârlılık henüz doğrulanmış değildir.

İşlem geçmişi dosyası GitHub’a dahil değildir. Kurtarma verisi yalnız yetkili Supabase tablolarında saklanır; testler sentetik veriler kullanır.
