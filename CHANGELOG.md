# Changelog

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
