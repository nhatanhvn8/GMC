# Changelog

## [1.0.10] - 2026-03-05
- Script-first flow: ưu tiên rule/script, giảm phụ thuộc AI trong detect login state
- Tăng nhận diện DOM đa ngôn ngữ và ổn định hơn cho login/recovery challenge

## [1.0.9] - 2026-03-05
- Bump version

## [1.0.8] - 2026-03-05
- Nhận diện trang hoạt động với mọi ngôn ngữ UI (EN/VI/TH/JA/KO/ZH/FR/DE/ES/...)
- Thay thế toàn bộ body text detection bằng DOM structure: maxlength, inputmode, autocomplete, URL, form action, data-challengetype
- _classify_verify_challenge_kind: bỏ body text, tăng trọng số URL + structural signals
- _is_2fa_challenge_page: bỏ English body text fallback, dùng maxlength=6 + JS timer/countdown
- _is_phone_number_challenge_page: bỏ English text, dùng country selector + form action
- _is_verify_recovery_page: ưu tiên URL + email input structure
- _is_couldnt_sign_in_page: thêm URL signal + DOM error container
- _is_email_entry_page: thuần DOM (identifier input)
- _is_post_login_setup_page: thuần DOM (Skip button jsname + page structure)
- _is_recovery_choice_page: bỏ text fallback, dùng jsname/jscontroller structural count
- _has_validation_error: dùng aria-invalid + role=alert trước text
- _detect_account_state: thêm multilingual keywords cho restricted/suspicious/too_many
- _detect_login_page_kind_once Layer 5: thay body text bằng structural analysis hoàn toàn

## [1.0.3] - 2026-03-05
- GUI mở tối đa hóa (zoomed) khi khởi động, vừa màn hình 1920x1200
- Cột bảng Dashboard tự scale theo độ rộng màn hình
- Settings tab giãn full width, hỗ trợ scroll chuột
- Status bar gọn hơn (4 dòng, có tiêu đề)

## [1.0.2] - 2026-03-05
- Thêm ô nhập Anthropic API Key trong Settings (AI fallback)
- Key tự load từ config khi khởi động, không cần sửa file thủ công

## [1.0.1] - 2026-03-05
- Thêm tùy chọn "Không lưu profile": xóa cache browser sau mỗi session
- Hỗ trợ cả GoLogin, GPM và Chrome mode

## [1.0.0] - 2026-03-06
- Init: Gmail Tool public release
- Auto-update từ GitHub khi khởi động
- DOM-based AI page detection (Claude Haiku fallback)
- Local sklearn model pipeline (train_dom_model.py)
- Multi-thread browser automation (GoLogin Orbita 139/143)
- Rotating proxy support với random selection
- 2Captcha image captcha solver
- Status detection: wrong_password, password_changed, wrong_2fa, too_many_attempts...
- Delete by status feature
- Auto-save settings
