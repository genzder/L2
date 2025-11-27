import os
import pandas as pd
from pathlib import Path
import shutil

base_dir = Path("/dataset")
train_csv = "/archive/Train.csv"
test_csv = "/archive/Test.csv"
images_source_train = Path("/archive")
images_source_test = Path("/archive")

# ALLOWED_CLASSES = {0, 1, 2, 3, 4, 5, 7, 8}

for split in ["train", "val"]:
    (base_dir / "images" / split).mkdir(parents=True, exist_ok=True)
    (base_dir / "labels" / split).mkdir(parents=True, exist_ok=True)

def convert_to_yolo(x1, y1, x2, y2, img_w, img_h):
    x_center = (x1 + x2) / 2 / img_w
    y_center = (y1 + y2) / 2 / img_h
    width = (x2 - x1) / img_w
    height = (y2 - y1) / img_h
    return x_center, y_center, width, height

# Обработка CSV
def process_csv(csv_path, split, source_img_dir):
    count = 0
    df = pd.read_csv(csv_path)
    for _, row in df.iterrows():
        class_id = row["ClassId"]

        # if class_id not in ALLOWED_CLASSES:
        #     print(f" Пропущено: {class_id} класс не в списке")
        #     continue
        
        img_rel_path = row["Path"]
        src_img_path = source_img_dir / img_rel_path
        if not src_img_path.exists():
            print(f" Пропущено: {src_img_path} не найден")
            continue

        dst_img_path = base_dir / "images" / split / img_rel_path.replace("/", "_")
        dst_img_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src_img_path, dst_img_path)

        img_w, img_h = row["Width"], row["Height"]
        x1, y1, x2, y2 = row["Roi.X1"], row["Roi.Y1"], row["Roi.X2"], row["Roi.Y2"]
        class_id = row["ClassId"]

        x_center, y_center, w, h = convert_to_yolo(x1, y1, x2, y2, img_w, img_h)

        dst_labels_path = base_dir / "labels" / split / img_rel_path.replace("/", "_")
        label_path = dst_labels_path.with_suffix(".txt")
        with open(label_path, "a") as f:
            f.write(f"{class_id} {x_center:.6f} {y_center:.6f} {w:.6f} {h:.6f}\n")
        
        count = count + 1
            
    print(f"count:{count}")

process_csv(train_csv, "train", images_source_train)
process_csv(test_csv, "val", images_source_test)

print(" Данные сконвертированы в формат YOLO!")