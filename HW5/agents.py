import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
import gymnasium as gym
from gymnasium import spaces
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import random
import logging

# =========================
# Logging
# =========================
logging.basicConfig(
    filename="training.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

DEVICE = torch.device("cpu")

# =========================
# Environment
# =========================
class HideAndSeekEnv(gym.Env):
    metadata = {"render_modes": ["rgb_array"]}

    def __init__(self, size=25, wall_prob=0.15, max_steps=300):
        super().__init__()
        self.size = size
        self.wall_prob = wall_prob
        self.max_steps = max_steps
        self.action_space = spaces.Discrete(5)
        self.step_count = 0
        self.grid = None
        self.pos_A = None
        self.pos_B = None

    def reset(self, seed=None):
        super().reset(seed=seed)
        self.step_count = 0
        self._generate_map()
        self._place_agents()
        return self._get_obs(), {}

    def _generate_map(self):
        while True:
            self.grid = np.zeros((self.size, self.size), dtype=np.int8)
            for i in range(self.size):
                for j in range(self.size):
                    if random.random() < self.wall_prob:
                        self.grid[i, j] = 1
            self.grid[0, 0] = 0
            if self._is_connected():
                break

    def _is_connected(self):
        visited = set()
        stack = [(0, 0)]
        while stack:
            x, y = stack.pop()
            if (x, y) in visited:
                continue
            visited.add((x, y))
            for dx, dy in [(-1,0),(1,0),(0,-1),(0,1)]:
                nx, ny = x+dx, y+dy
                if 0 <= nx < self.size and 0 <= ny < self.size:
                    if self.grid[nx, ny] == 0:
                        stack.append((nx, ny))
        return len(visited) > self.size * self.size * 0.7

    def _place_agents(self):
        free = list(zip(*np.where(self.grid == 0)))
        self.pos_A = random.choice(free)
        self.pos_B = random.choice(free)
        while self.pos_B == self.pos_A:
            self.pos_B = random.choice(free)

    def _move(self, pos, action):
        dx, dy = [( -1,0),(1,0),(0,-1),(0,1),(0,0)][action]
        nx, ny = pos[0]+dx, pos[1]+dy
        if 0 <= nx < self.size and 0 <= ny < self.size:
            if self.grid[nx, ny] == 0:
                return (nx, ny)
        return pos

    def _get_local_obs(self, pos, radius, other_pos):
        R = radius
        size = 2 * R + 1
        obs = np.zeros((size, size, 4), dtype=np.float32)
        for dx in range(-R, R+1):
            for dy in range(-R, R+1):
                x, y = pos[0]+dx, pos[1]+dy
                ix, iy = dx+R, dy+R
                if 0 <= x < self.size and 0 <= y < self.size:
                    obs[ix, iy, 0] = self.grid[x, y]
                else:
                    obs[ix, iy, 1] = 1
        obs[R, R, 2] = 1
        if abs(other_pos[0]-pos[0]) <= R and abs(other_pos[1]-pos[1]) <= R:
            ox = other_pos[0] - pos[0] + R
            oy = other_pos[1] - pos[1] + R
            obs[ox, oy, 3] = 1
        return obs

    def _get_obs(self):
        obs_A = self._get_local_obs(self.pos_A, 2, self.pos_B)
        obs_B = self._get_local_obs(self.pos_B, 3, self.pos_A)
        return obs_A, obs_B

    def step(self, actions):
        self.step_count += 1
        self.pos_A = self._move(self.pos_A, actions[0])
        self.pos_B = self._move(self.pos_B, actions[1])
        done = False
        reward_A = -0.01
        reward_B = 0.01
        if self.pos_A == self.pos_B:
            reward_A += 1.0
            reward_B -= 1.0
            done = True
        if self.step_count >= self.max_steps:
            done = True
        return self._get_obs(), (reward_A, reward_B), done, False, {}

    def render(self):
        img = np.ones((self.size, self.size, 3))
        img[self.grid == 1] = [0,0,0]
        img[self.pos_A] = [1,0,0]  # красный охотник
        img[self.pos_B] = [0,0,1]  # синий хидер
        return img

# =========================
# PPO + LSTM Network
# =========================
class ActorCritic(nn.Module):
    def __init__(self, obs_shape):
        super().__init__()
        C = obs_shape[2]
        self.conv = nn.Sequential(
            nn.Conv2d(C, 16, 3),
            nn.ReLU(),
            nn.Conv2d(16, 32, 3),
            nn.ReLU()
        )
        with torch.no_grad():
            dummy = torch.zeros(1, C, obs_shape[0], obs_shape[1])
            n_flat = self.conv(dummy).view(1, -1).size(1)
        self.lstm = nn.LSTM(n_flat, 64, batch_first=True)
        self.actor = nn.Linear(64, 5)
        self.critic = nn.Linear(64, 1)

    def forward(self, x, hidden):
        B, T, H, W, C = x.shape
        x = x.view(B*T, C, H, W)
        x = self.conv(x)
        x = x.view(B, T, -1)
        x, hidden = self.lstm(x, hidden)
        logits = self.actor(x)
        value = self.critic(x)
        return logits, value.squeeze(-1), hidden

class PPOAgent:
    def __init__(self, obs_shape, lr=2.5e-4):
        self.model = ActorCritic(obs_shape).to(DEVICE)
        self.optimizer = optim.Adam(self.model.parameters(), lr=lr)

    def act(self, obs, hidden):
        obs = torch.tensor(obs, dtype=torch.float32).unsqueeze(0).unsqueeze(0)
        logits, value, hidden = self.model(obs, hidden)
        dist = torch.distributions.Categorical(logits=logits.squeeze(0))
        action = dist.sample()
        return action.item(), dist.log_prob(action), value.squeeze(), hidden

# =========================
# Training Functions
# =========================
def train_model(episodes=500, max_steps=300, save_every=50,
                save_path_A="agent_A.pth", save_path_B="agent_B.pth",
                map_size=25):
    env = HideAndSeekEnv(size=map_size, max_steps=max_steps)
    agent_A = PPOAgent((5,5,4))
    agent_B = PPOAgent((7,7,4))

    for ep in range(episodes):
        obs, _ = env.reset()
        hA = (torch.zeros(1,1,64), torch.zeros(1,1,64))
        hB = (torch.zeros(1,1,64), torch.zeros(1,1,64))
        total_A, total_B = 0.0, 0.0
        seen_B, step_seen = False, None

        for step in range(max_steps):
            aA, _, _, hA = agent_A.act(obs[0], hA)
            aB, _, _, hB = agent_B.act(obs[1], hB)
            obs, rewards, done, _, _ = env.step((aA, aB))
            total_A += rewards[0]
            total_B += rewards[1]

            visible_B = np.any(obs[0][...,3]==1)
            if visible_B and not seen_B:
                seen_B, step_seen = True, step
                logging.info(f"Episode {ep}: Agent A FIRST saw Agent B at step {step}")

            if done and rewards[0]>0:
                logging.info(f"Episode {ep}: Agent A CAUGHT Agent B at step {step}")
                if step_seen is not None:
                    logging.info(f"Episode {ep}: Catch delay = {step-step_seen} steps")

            if done: break

        logging.info(f"Episode {ep} finished | Total reward A={total_A:.2f}, B={total_B:.2f}")

        if (ep+1) % save_every == 0 or ep == episodes-1:
            torch.save(agent_A.model.state_dict(), f"size_{map_size}_EP_{ep+1}_" + save_path_A)
            torch.save(agent_B.model.state_dict(), f"size_{map_size}_EP_{ep+1}_" + save_path_B)
            logging.info(f"Models saved at episode {ep+1}")

def continue_train_model(existing_A="agent_A.pth", existing_B="agent_B.pth",
                         episodes=500, start_episode=0, max_steps=300,
                         save_every=50, map_size=25):
    env = HideAndSeekEnv(size=map_size, max_steps=max_steps)
    agent_A = PPOAgent((5,5,4))
    agent_B = PPOAgent((7,7,4))
    agent_A.model.load_state_dict(torch.load(existing_A, map_location=DEVICE))
    agent_B.model.load_state_dict(torch.load(existing_B, map_location=DEVICE))

    for ep in range(start_episode, start_episode+episodes):
        obs, _ = env.reset()
        hA = (torch.zeros(1,1,64), torch.zeros(1,1,64))
        hB = (torch.zeros(1,1,64), torch.zeros(1,1,64))
        total_A, total_B = 0.0, 0.0
        seen_B, step_seen = False, None

        for step in range(max_steps):
            aA, _, _, hA = agent_A.act(obs[0], hA)
            aB, _, _, hB = agent_B.act(obs[1], hB)
            obs, rewards, done, _, _ = env.step((aA, aB))
            total_A += rewards[0]
            total_B += rewards[1]

            visible_B = np.any(obs[0][...,3]==1)
            if visible_B and not seen_B:
                seen_B, step_seen = True, step
                logging.info(f"Episode {ep}: Agent A FIRST saw Agent B at step {step}")

            if done and rewards[0]>0:
                logging.info(f"Episode {ep}: Agent A CAUGHT Agent B at step {step}")
                if step_seen is not None:
                    logging.info(f"Episode {ep}: Catch delay = {step-step_seen} steps")

            if done: break

        logging.info(f"Episode {ep} finished | Total reward A={total_A:.2f}, B={total_B:.2f}")

        if (ep+1) % save_every == 0 or ep == start_episode+episodes-1:
            torch.save(agent_A.model.state_dict(), f"size_{map_size}_EP_{ep+1}_" + existing_A)
            torch.save(agent_B.model.state_dict(), f"size_{map_size}_EP_{ep+1}_" + existing_B)
            logging.info(f"Models saved at episode {ep+1}")



# =========================
# Visualization
# =========================
def render_model(
    model_A="agent_A.pth",
    model_B="agent_B.pth",
    steps=300,
    fps=5,
    save_name="hide_and_seek.mp4",
    map_size=25
):
    env = HideAndSeekEnv(size=map_size, max_steps=steps)

    agent_A = PPOAgent((5,5,4))
    agent_B = PPOAgent((7,7,4))

    agent_A.model.load_state_dict(torch.load(model_A, map_location=DEVICE))
    agent_B.model.load_state_dict(torch.load(model_B, map_location=DEVICE))

    obs, _ = env.reset()

    hA = (torch.zeros(1,1,64), torch.zeros(1,1,64))
    hB = (torch.zeros(1,1,64), torch.zeros(1,1,64))

    frames = []
    texts = []
    text_colors = []
    end_flags = []

    total_A = 0.0
    total_B = 0.0

    caught_frame_idx = None
    caught = False
    collapse_steps = 10  # количество шагов анимации схлопывания

    # ===== основной эпизод =====
    for step in range(steps):
        # === шаг среды ===
        with torch.no_grad():
            aA, _, _, hA = agent_A.act(obs[0], hA)
            aB, _, _, hB = agent_B.act(obs[1], hB)

        obs, rewards, done, _, _ = env.step((aA, aB))

        total_A += rewards[0]
        total_B += rewards[1]

        # === рендер после шага ===
        img = env.render()

        if done and rewards[0] > 0 and not caught:
            caught = True
            caught_frame_idx = len(frames)

        text = (
            f"Step: {step}\n"
            f"Hunter(red) reward: {rewards[0]:+.2f} | Total: {total_A:+.2f}\n"
            f"Hider       reward: {rewards[1]:+.2f} | Total: {total_B:+.2f}"
        )
        if caught:
            text += "\nCAUGHT!"

        frames.append(img)
        texts.append(text)
        text_colors.append("red" if caught else "black")
        end_flags.append(False)

        if caught:
            break

    # ===== анимация схлопывания =====
    if caught_frame_idx is not None:
        pos_A = env.pos_A
        pos_B = env.pos_B
        for step in range(1, collapse_steps+1):
            alpha = 1.0 - step / collapse_steps
            img = env.render()
            # линейно уменьшаем цвета агентов к черному
            img[pos_A] = np.array([1,0,0])
            img[pos_B] = np.array([alpha,0,0])
            frames.append(img)
            texts.append(texts[-1])
            text_colors.append("red")
            end_flags.append(False)

        # ===== 5 секунд финального экрана =====
        extra_frames = fps * 5
        last_frame = frames[-1]
        last_text = texts[-1]
        for _ in range(extra_frames):
            frames.append(last_frame)
            texts.append(last_text)
            text_colors.append("red")
            end_flags.append(True)

    # ======= Animation =======
    fig, ax = plt.subplots(figsize=(6, 6))
    plt.subplots_adjust(top=0.78)

    im = ax.imshow(frames[0])

    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_color("black")
        spine.set_linewidth(1.2)

    txt = ax.text(
        0.01, 1.2,
        texts[0],
        transform=ax.transAxes,
        fontsize=11,
        color=text_colors[0],
        verticalalignment="top"
    )

    end_txt = ax.text(
        0.5, 0.5,
        "",
        transform=ax.transAxes,
        fontsize=32,
        color="red",
        ha="center",
        va="center",
        weight="bold"
    )

    def update(i):
        im.set_data(frames[i])
        txt.set_text(texts[i])
        txt.set_color(text_colors[i])

        if end_flags[i]:
            end_txt.set_text("КОНЕЦ!")
        else:
            end_txt.set_text("")

        return [im, txt, end_txt]

    ani = animation.FuncAnimation(
        fig,
        update,
        frames=len(frames),
        interval=1000 // fps,
        blit=False
    )

    ani.save(save_name, fps=fps)
    plt.close()



# =========================
# Main
# =========================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--train", action="store_true", help="Train agents from scratch")
    parser.add_argument("--continue_train", action="store_true", help="Continue training agents")
    parser.add_argument("--render", action="store_true", help="Render trained agents")
    parser.add_argument("--episodes", type=int, default=500, help="Number of episodes")
    parser.add_argument("--max_steps", type=int, default=300, help="Max steps per episode")
    parser.add_argument("--save_every", type=int, default=50, help="Save models every N episodes")
    parser.add_argument("--existing_A", type=str, default="agent_A.pth")
    parser.add_argument("--existing_B", type=str, default="agent_B.pth")
    parser.add_argument("--start_episode", type=int, default=0)
    parser.add_argument("--steps", type=int, default=300, help="Steps for rendering")
    parser.add_argument("--fps", type=int, default=5, help="FPS for rendering")
    parser.add_argument("--save_name", type=str, default="demo.mp4")
    parser.add_argument("--map_size", type=int, default=25, help="Size of the map")

    args = parser.parse_args()

    if args.train:
        logging.info("Starting training from scratch...")
        train_model(episodes=args.episodes, max_steps=args.max_steps,
                    save_every=args.save_every, map_size=args.map_size)

    if args.continue_train:
        logging.info("Continuing training...")
        continue_train_model(existing_A=args.existing_A, existing_B=args.existing_B,
                             episodes=args.episodes, start_episode=args.start_episode,
                             max_steps=args.max_steps, save_every=args.save_every,
                             map_size=args.map_size)

    if args.render:
        logging.info("Rendering episode...")
        render_model(model_A=args.existing_A, model_B=args.existing_B,
                     steps=args.steps, fps=args.fps,
                     save_name=args.save_name, map_size=args.map_size)
