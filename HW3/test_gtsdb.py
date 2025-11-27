from ultralytics import YOLO
import cv2
import os
import numpy as np


model = YOLO("/home/gr/ML/L2/HW3/sing/dop/runs/detect/gtsdb_yolov8n12/weights/best.pt")
# video_path = "/home/gr/ML/L2/HW3/sing/imput_yolo_light.mp4"
video_path = "/home/gr/ML/L2/HW3/sing/imput_yolo.mp4"
output_path = "/home/gr/ML/L2/HW3/sing/output_with_labels_7.mp4"
conf_thresh = 0.5

# Папка с иконками (должна содержать файлы: 0.png, 26.png, 28.png и т.д.)
ICONS_DIR = "/home/gr/ML/L2/HW3/sing/dop/dataset/Meta/"  # ← создайте эту папку!

cap = cv2.VideoCapture(video_path)
if not cap.isOpened():
    raise FileNotFoundError(f"Не удалось открыть видео: {video_path}")

fps = int(cap.get(cv2.CAP_PROP_FPS))
width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
out = cv2.VideoWriter(output_path, cv2.VideoWriter_fourcc(*'mp4v'), fps, (width, height))

seen_classes = set()
icon_cache = {}  # кэш загруженных иконок

# Загружаем иконки один раз
def load_icon(class_id, size=(64, 64)):
    icon_path = os.path.join(ICONS_DIR, f"{class_id}.png")
    if os.path.isfile(icon_path):
        icon = cv2.imread(icon_path, cv2.IMREAD_UNCHANGED)
        if icon is None:
            print(f"Не удалось загрузить иконку: {icon_path}")
            return None
        # Изменяем размер
        icon = cv2.resize(icon, size, interpolation=cv2.INTER_AREA)
        return icon
    else:
        print(f"Иконка не найдена: {icon_path}")
        return None

# Функция наложения иконки с прозрачностью
def overlay_icon(frame, icon, x, y):
    if icon.shape[2] == 4:  # есть альфа-канал
        alpha_icon = icon[:, :, 3] / 255.0
        alpha_frame = 1.0 - alpha_icon

        for c in range(3):
            frame[y:y+icon.shape[0], x:x+icon.shape[1], c] = (
                alpha_icon * icon[:, :, c] +
                alpha_frame * frame[y:y+icon.shape[0], x:x+icon.shape[1], c]
            )
    else:  # без прозрачности
        frame[y:y+icon.shape[0], x:x+icon.shape[1]] = icon

# ----------------------------
# ОСНОВНОЙ ЦИКЛ
# ----------------------------
while True:
    ret, frame = cap.read()
    if not ret:
        break

    # Детекция
    results = model(frame, conf=conf_thresh)
    result = results[0]

    # Собираем уникальные классы
    if result.boxes is not None:
        class_ids = result.boxes.cls.cpu().numpy().astype(int)
        seen_classes.update(class_ids)

    # Рисуем стандартные рамки
    annotated_frame = result.plot()

    # Накладываем иконки
    icon_size = 64
    padding = 10
    x_start = padding
    y_start = padding

    for i, class_id in enumerate(sorted(seen_classes)):
        if class_id not in icon_cache:
            icon = load_icon(class_id, (icon_size, icon_size))
            if icon is not None:
                icon_cache[class_id] = icon
            else:
                continue

        icon = icon_cache[class_id]
        x = x_start
        y = y_start + i * (icon_size + padding)

        # Проверка, чтобы не выйти за границы кадра
        if y + icon_size > annotated_frame.shape[0]:
            break

        overlay_icon(annotated_frame, icon, x, y)

    out.write(annotated_frame)

# ----------------------------
# ЗАВЕРШЕНИЕ
# ----------------------------
cap.release()
out.release()
print(" Готово! Обнаружены классы:", [model.names[cid] for cid in sorted(seen_classes)])