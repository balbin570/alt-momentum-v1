# İlk küçük C değişikliği — dağıtım doğrulaması

16 Kasım 2026. Kullanıcı küçük paper strateji değişikliklerini onayladı ve Render için Mucahit's workspace / mucahit bağlantısını seçti. Hedef servis alt-momentum-v1, srv-db0h6jou01pc73ac0ghg, workspace tea-dak3m7ek1f9s73asjf1g, repo balbin570/alt-momentum-v1 main. autoDeploy=yes; ikinci manuel dağıtım tetiklenmedi.

Commit 2035fd7326e6eb75c25fb02b91403892149f00bd ana dala beklenen önceki SHA ce0ff46af4c05ba03eccd1efa5b79d2dfe5be3a1 kontrolüyle alındı. Deploy dep-dbtffo68bjmc73fk563g 2026-11-16T12:33:24.360357Z (15:33 Türkiye) live olarak doğrulandı.

Tek strateji değişikliği: yeni C girişlerinde breakout üst sınırı %1.50 -> %0.50; alt sınır %0.20. Yeni giriş etiketi C_ENTRY_BREAKOUT050_V1. c_rule_version=C_V2_PROTECT çıkış sürümü aynıdır. Top20 ve Hızlı değişmedi; emekli C shadow yeniden açılmadı. Diğer giriş filtreleri, çıkışlar, pozisyon büyüklüğü/sayısı, maliyet varsayımları ve gerçek emir yasağı aynı.

Doğrulama: /c-entry-experiment HTTP 200, entry_version=C_ENTRY_BREAKOUT050_V1, breakout_range_pct=[0.2,0.5], orders=false,trading=false. Yeni kohort ilk kontrolde closed=0/open=0, baseline_v2 closed=67/open=1. /c-loss-analysis.active_c_entry_version ve c_entry_cohorts yeni sürümü doğru gösteriyor. Top20 status OK, TOP20_SCALP_V5, auto_running=true ve hybrid_entries_enabled=false.

Kalıcılık: 1934 kayıt restore edildi, 1933 kapalı kaydın TÜM alanları dağıtım öncesiyle birebir aynı; önceki açık işlemin giriş/çıkış sürümleri aynı. /v915-storage-status RESTORED/OK, son başarılı yazım 12:33:42 UTC revision10380; 12:34:33'e kadar Render error seviyesinde log yok. Kurtarma metadatasındaki 16 eski kayıp kayıt bilgisi değiştirilmedi; yeni veri kaybı iddiası yok.

Beş yeni hedefli bütünleşme testi ve py_compile geçti. Tüm paket başlangıçta 31, yama sonrası 36 sonuç üretti; aynı üç eski shadow başarısızlığı ve aynı eksik select_leaders import hatası var. Yeni hata yok, tüm paket başarılı iddiası yok. Detay ve rollback: main docs/C_ENTRY_BREAKOUT050_V1.md.

Kârlılık henüz doğrulanmadı. Önceki 7gün/50işlem koşulu kullanıcı tarafından küçük paper denemesini başlatmak için kaldırıldı; başarı veya ölçeği artırma kanıtı değildir. Yeni kohort ayrı izlenecek; yeterli yeni veri olmadan ikinci parametre değiştirilmeyecek. Baseline tarihseldir ve eşzamanlı kontrol değildir; farklı piyasa dönemiyle doğrudan nedensel karşılaştırma yapılmaz.

8 saatlik otomasyon doğrulanmış çalışma alanı, servis, commit ve yeni c_entry_version gruplamasını kullanacak şekilde güncellendi. Workspace seçimini tekrar sormaması ve zaten devredeki denemeyi yeniden dağıtmaması kaydedildi. Yeni girişler sonuç ürettikçe net beklenti, PF, kazanma, tutma süresi, düşüş, sembol/zaman yoğunluğu ve kayıt sağlığı incelenmeli. Rollback halinde yalnız sonraki girişler ayrı sürümle eski aralığa döner; tarihsel kayıtlar ve açık işlemlerin V2 çıkışları korunur.
