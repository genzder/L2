Hide and Seek AI - Short Guide

На визуализации используются:
- Охотник (Hunter) – красный
- Хидер (Hider) – синий
- Стены – черные
- Пустые клетки – белые


1. Обучение
python agents.py --train --episodes 500 --max_steps 300 --save_every 50 --map_size 25

 --train : начать обучение с нуля
 --episodes : количество эпизодов
 --max_steps : максимальные шаги в эпизоде
 --save_every : сохранять модели каждые N эпизодов
 --map_size : размер карты



2. Продолжение обучения
python agents.py --continue_train --existing_A agent_A.pth --existing_B agent_B.pth --episodes 500 --start_episode 50 --max_steps 300 --save_every 50 --map_size 25

 --continue_train : продолжить обучение
 --existing_A / --existing_B : существующие модели
 --start_episode : с какого эпизода продолжать



3. Визуализация
python agents.py --render --existing_A agent_A.pth --existing_B agent_B.pth --steps 300 --fps 5 --save_name demo.mp4 --map_size 25

 --render : создать видео
 --existing_A / --existing_B : существующие модели
 --steps : количество шагов
 --fps : кадры в секунду
 --save_name : имя видео
 --map_size : размер карты
