from ultralytics import YOLO

model = YOLO("yolov8n.pt")  

# Обучаем
results = model.train(
    data="data.yaml",
    epochs=50,
    imgsz=640,
    batch=16,          
    name="gtsdb_yolov8n",
    device="cpu"      
)