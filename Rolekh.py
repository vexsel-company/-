import os
import sqlite3
import json
import time
import threading
import random
import re
import threading
import requests
import pylint.lint
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Union


# Загружаем конфигурацию из config.json
def load_config():
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ Файл config.json не найден! Создайте его по примеру.")
        exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Ошибка в формате config.json: {e}")
        exit(1)

CONFIG = load_config()

# Настройки бота из конфигурации
TOKEN = CONFIG['vk_token']
GROUP_ID = CONFIG['group_id']
GRAND_DEVELOPER_ID = CONFIG['grand_developer']['user_id']
GRAND_DEVELOPER_USERNAME = CONFIG['grand_developer']['username']

# Глобальные переменные
VK_TOKEN = TOKEN
VK_GROUP_ID = GROUP_ID
GRAND_MANAGER_ID = None
ROULETTE_TIMERS = {}
CRASH_TIMERS = {}
DICE_TIMERS = {}
PIAR_TIMERS = {}

class Database:
    def __init__(self):
        self.db_path = CONFIG.get('database_path', 'bot_lox.sqlite')
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.initialize_tables()
        print("Подключение к SQLite базе данных успешно.")

    def initialize_tables(self):
        cursor = self.conn.cursor()

        # Создаем индексы для оптимизации запросов по чатам
        self.create_chat_specific_indexes()

        # Таблица пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                nickname TEXT,
                role_level INTEGER DEFAULT 0,
                message_count INTEGER DEFAULT 0,
                join_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                invited_by INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица предупреждений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                reason TEXT,
                warned_by INTEGER,
                chat_id INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица мутов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mutes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                reason TEXT,
                muted_by INTEGER,
                mute_until DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')

        # Таблица банов чата
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chat_bans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                reason TEXT,
                banned_by INTEGER,
                banned_until DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')

        # Добавляем колонку banned_until если её нет (миграция для существующих БД)
        try:
            cursor.execute('SELECT banned_until FROM chat_bans LIMIT 1')
        except sqlite3.OperationalError:
            cursor.execute('ALTER TABLE chat_bans ADD COLUMN banned_until DATETIME')

        # Таблица системных администраторов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_admins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE,
                username TEXT,
                access_level INTEGER,
                granted_by INTEGER,
                granted_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица тикетов поддержки
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS support_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                chat_id INTEGER,
                message TEXT,
                assigned_to INTEGER,
                status TEXT DEFAULT 'open',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                closed_at DATETIME
            )
        ''')

        # Таблица системных банов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_bans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                reason TEXT,
                banned_by INTEGER,
                banned_until DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')

        # Таблица балансов пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_balances (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER DEFAULT 0,
                bonus_points INTEGER DEFAULT 0,
                last_bonus_claim DATETIME DEFAULT CURRENT_TIMESTAMP,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица ролей в чатах
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chat_roles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                role_level INTEGER DEFAULT 0,
                role_name TEXT,
                granted_by INTEGER,
                granted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                UNIQUE(user_id, chat_id)
            )
        ''')

        # Таблица определений кастомных ролей для чатов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS custom_role_definitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                role_level INTEGER,
                role_name TEXT,
                created_by INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                UNIQUE(chat_id, role_level)
            )
        ''')

        # Таблица иммунитетов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS immunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                granted_by INTEGER,
                granted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')

        # Таблица никнеймов по чатам
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chat_nicknames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                nickname TEXT,
                set_by INTEGER,
                set_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                UNIQUE(user_id, chat_id)
            )
        ''')

        # Таблица игр рулетки
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS roulette_games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                status TEXT DEFAULT 'active',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                ended_at DATETIME,
                winning_number INTEGER,
                is_active BOOLEAN DEFAULT 1
            )
        ''')

        # Таблица ставок рулетки
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS roulette_bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER,
                user_id INTEGER,
                username TEXT,
                bet_type TEXT,
                bet_target TEXT,
                bet_amount INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (game_id) REFERENCES roulette_games (id)
            )
        ''')

        # Таблица игр Crash
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crash_games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                status TEXT DEFAULT 'active',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                ended_at DATETIME,
                crash_multiplier REAL,
                is_active BOOLEAN DEFAULT 1
            )
        ''')

        # Таблица ставок Crash
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crash_bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER,
                user_id INTEGER,
                username TEXT,
                bet_amount INTEGER,
                target_multiplier REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (game_id) REFERENCES crash_games (id)
            )
        ''')

        # Таблица игр в кости
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dice_games (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                creator_id INTEGER,
                creator_username TEXT,
                bet_amount INTEGER,
                max_players INTEGER DEFAULT 2,
                status TEXT DEFAULT 'waiting',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                ended_at DATETIME,
                winner_id INTEGER,
                is_active BOOLEAN DEFAULT 1
            )
        ''')

        # Таблица участников игры в кости
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dice_players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER,
                user_id INTEGER,
                username TEXT,
                dice_result INTEGER,
                joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (game_id) REFERENCES dice_games (id)
            )
        ''')

        # Таблица зарегистрированных бесед
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS registered_chats (
                chat_id INTEGER PRIMARY KEY,
                is_registered BOOLEAN DEFAULT 0,
                registered_by INTEGER,
                registration_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                owner_id INTEGER,
                title TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица настроек бота
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица объединений конференций
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chat_unions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                union_key TEXT UNIQUE,
                union_name TEXT,
                created_by INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица чатов в объединениях
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS union_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                union_id INTEGER,
                chat_id INTEGER,
                added_by INTEGER,
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (union_id) REFERENCES chat_unions (id),
                UNIQUE(union_id, chat_id)
            )
        ''')

        # Таблица кастомных прав доступа к командам
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS command_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                command TEXT,
                required_level INTEGER,
                set_by INTEGER,
                set_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, command)
            )
        ''')

        # Таблица запрещенных слов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS filtered_words (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                word TEXT,
                added_by INTEGER,
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, word)
            )
        ''')

        self.conn.commit()
        print("Таблицы базы данных инициализированы.")

        # Создаем индексы после создания таблиц
        self.create_chat_specific_indexes()

    def create_chat_specific_indexes(self):
        """Создает индексы для оптимизации запросов по чатам"""
        cursor = self.conn.cursor()

        # Индексы для ускорения поиска по чатам
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_chat_roles_chat_id ON chat_roles(chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_chat_roles_user_chat ON chat_roles(user_id, chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_custom_role_definitions_chat ON custom_role_definitions(chat_id, role_level)',
            'CREATE INDEX IF NOT EXISTS idx_warnings_chat_id ON warnings(chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_mutes_chat_id ON mutes(chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_chat_bans_chat_id ON chat_bans(chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_roulette_games_chat_id ON roulette_games(chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_crash_games_chat_id ON crash_games(chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_dice_games_chat_id ON dice_games(chat_id)',
            'CREATE INDEX IF NOT EXISTS idx_dice_players_game_id ON dice_players(game_id)'
        ]

        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
            except Exception as e:
                print(f"Предупреждение при создании индекса: {e}")

        self.conn.commit()
        print("Индексы для чат-специфичных данных созданы.")

    def get_chat_custom_roles(self, chat_id):
        """Получает все кастомные роли для конкретного чата"""
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT role_level, role_name FROM custom_role_definitions WHERE chat_id = ? AND is_active = 1 ORDER BY role_level DESC',
            (chat_id,)
        )
        return cursor.fetchall()

    def get_user(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        return cursor.fetchone()

    def create_or_update_user(self, user_id, username=None, nickname=None, role_level=None, invited_by=None):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO users 
            (user_id, username, nickname, role_level, invited_by, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (user_id, username, nickname, role_level or 0, invited_by))
        self.conn.commit()

    def add_warning(self, user_id, reason, warned_by, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT INTO warnings (user_id, reason, warned_by, chat_id) VALUES (?, ?, ?, ?)',
            (user_id, reason, warned_by, chat_id)
        )
        self.conn.commit()

    def get_user_warnings(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM warnings WHERE user_id = ? ORDER BY created_at DESC', (user_id,))
        return cursor.fetchall()

    def add_mute(self, user_id, chat_id, reason, muted_by, mute_until):
        cursor = self.conn.cursor()
        # Деактивируем старые муты
        cursor.execute(
            'UPDATE mutes SET is_active = 0 WHERE user_id = ? AND chat_id = ?',
            (user_id, chat_id)
        )
        # Добавляем новый мут
        cursor.execute(
            'INSERT INTO mutes (user_id, chat_id, reason, muted_by, mute_until) VALUES (?, ?, ?, ?, ?)',
            (user_id, chat_id, reason, muted_by, mute_until.isoformat())
        )
        self.conn.commit()

    def get_active_mute_in_chat(self, user_id, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM mutes WHERE user_id = ? AND chat_id = ? AND is_active = 1 AND mute_until > datetime("now") ORDER BY created_at DESC LIMIT 1',
            (user_id, chat_id)
        )
        return cursor.fetchone()

    def remove_mute(self, user_id, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE mutes SET is_active = 0 WHERE user_id = ? AND chat_id = ? AND is_active = 1',
            (user_id, chat_id)
        )
        self.conn.commit()

    def add_chat_ban(self, user_id, chat_id, reason, banned_by):
        cursor = self.conn.cursor()
        # Деактивируем старые баны
        cursor.execute(
            'UPDATE chat_bans SET is_active = 0 WHERE user_id = ? AND chat_id = ?',
            (user_id, chat_id)
        )
        # Добавляем новый бан
        cursor.execute(
            'INSERT INTO chat_bans (user_id, chat_id, reason, banned_by) VALUES (?, ?, ?, ?)',
            (user_id, chat_id, reason, banned_by)
        )
        self.conn.commit()

    def get_user_ban_in_chat(self, user_id, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM chat_bans WHERE user_id = ? AND chat_id = ? AND is_active = 1 ORDER BY created_at DESC LIMIT 1',
            (user_id, chat_id)
        )
        return cursor.fetchone()

    def remove_chat_ban(self, user_id, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE chat_bans SET is_active = 0 WHERE user_id = ? AND chat_id = ? AND is_active = 1',
            (user_id, chat_id)
        )
        self.conn.commit()

    def get_system_admin(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM system_admins WHERE user_id = ?', (user_id,))
        return cursor.fetchone()

    def get_chat_role(self, user_id, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM chat_roles WHERE user_id = ? AND chat_id = ? AND is_active = 1',
            (user_id, chat_id)
        )
        return cursor.fetchone()

    def set_chat_role(self, user_id, chat_id, role_level, role_name, granted_by):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO chat_roles 
            (user_id, chat_id, role_level, role_name, granted_by, granted_at, is_active)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 1)
        ''', (user_id, chat_id, role_level, role_name, granted_by))
        self.conn.commit()

    def get_immunity(self, user_id, chat_id=None):
        cursor = self.conn.cursor()
        if chat_id:
            cursor.execute(
                'SELECT * FROM immunities WHERE user_id = ? AND chat_id = ? AND is_active = 1',
                (user_id, chat_id)
            )
        else:
            cursor.execute(
                'SELECT * FROM immunities WHERE user_id = ? AND chat_id IS NULL AND is_active = 1',
                (user_id,)
            )
        return cursor.fetchone()

    def create_support_ticket(self, user_id, username, chat_id, message):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT INTO support_tickets (user_id, username, chat_id, message) VALUES (?, ?, ?, ?)',
            (user_id, username, chat_id, message)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_user_balance(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM user_balances WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        if not result:
            cursor.execute('INSERT INTO user_balances (user_id, balance, bonus_points) VALUES (?, 0, 0)', (user_id,))
            self.conn.commit()
            return {'balance': 0, 'bonus_points': 0, 'last_bonus_claim': None}
        return dict(result)

    def set_user_balance(self, user_id, amount):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT OR IGNORE INTO user_balances (user_id) VALUES (?)',
            (user_id,)
        )
        cursor.execute(
            'UPDATE user_balances SET balance = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?',
            (amount, user_id)
        )
        self.conn.commit()

    def can_afford_bet(self, user_id, amount):
        balance = self.get_user_balance(user_id)
        return balance and balance['balance'] >= amount

    def claim_bonus(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE user_balances SET last_bonus_claim = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?',
            (user_id,)
        )
        self.conn.commit()

    def can_claim_bonus(self, user_id):
        balance_data = self.get_user_balance(user_id)
        if not balance_data:
            return True  # Если пользователя нет, то бонус можно получить

        last_claim_time = balance_data['last_bonus_claim']
        if not last_claim_time or last_claim_time == datetime.min:
            return True

        # Ensure last_claim_time is a datetime object for comparison
        if isinstance(last_claim_time, str):
            try:
                last_claim_time = datetime.fromisoformat(last_claim_time)
            except ValueError:
                return True # If parsing fails, allow bonus claim

        # Если прошел час с последнего получения бонуса
        return datetime.now() >= last_claim_time + timedelta(hours=1)

    def get_all_users_with_nicknames(self, chat_id=None):
        cursor = self.conn.cursor()
        if chat_id:
            cursor.execute(
                'SELECT user_id, nickname FROM chat_nicknames WHERE chat_id = ? AND is_active = 1',
                (chat_id,)
            )
        else:
            cursor.execute(
                'SELECT user_id, username, nickname FROM users WHERE nickname IS NOT NULL AND nickname != ""'
            )
        return cursor.fetchall()

    def get_all_users_without_nicknames(self, chat_id=None):
        cursor = self.conn.cursor()
        if chat_id:
            # Получаем пользователей из чата без никнеймов
            cursor.execute('''
                SELECT cr.user_id, u.username 
                FROM chat_roles cr
                LEFT JOIN chat_nicknames cn ON cr.user_id = cn.user_id AND cr.chat_id = cn.chat_id AND cn.is_active = 1
                LEFT JOIN users u ON cr.user_id = u.user_id
                WHERE cr.chat_id = ? AND cr.is_active = 1 AND cn.id IS NULL
            ''', (chat_id,))
        else:
            cursor.execute(
                'SELECT user_id, username FROM users WHERE nickname IS NULL OR nickname = ""'
            )
        return cursor.fetchall()

    def get_all_active_chat_bans(self, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM chat_bans WHERE chat_id = ? AND is_active = 1 ORDER BY created_at DESC',
            (chat_id,)
        )
        return cursor.fetchall()

    def remove_warning(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'DELETE FROM warnings WHERE user_id = ? ORDER BY created_at DESC LIMIT 1',
            (user_id,)
        )
        self.conn.commit()

    def get_user_tickets(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM support_tickets WHERE user_id = ? ORDER BY created_at DESC',
            (user_id,)
        )
        return cursor.fetchall()

    def set_immunity(self, user_id, granted_by, chat_id=None):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO immunities 
            (user_id, chat_id, granted_by, granted_at, is_active)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, 1)
        ''', (user_id, chat_id, granted_by))
        self.conn.commit()

    def remove_immunity(self, user_id, chat_id=None):
        cursor = self.conn.cursor()
        if chat_id:
            cursor.execute(
                'UPDATE immunities SET is_active = 0 WHERE user_id = ? AND chat_id = ? AND is_active = 1',
                (user_id, chat_id)
            )
        else:
            cursor.execute(
                'UPDATE immunities SET is_active = 0 WHERE user_id = ? AND chat_id IS NULL AND is_active = 1',
                (user_id,)
            )
        self.conn.commit()

    def get_all_immunities(self, chat_id=None):
        cursor = self.conn.cursor()
        if chat_id:
            cursor.execute(
                'SELECT * FROM immunities WHERE chat_id = ? AND is_active = 1 ORDER BY granted_at DESC',
                (chat_id,)
            )
        else:
            cursor.execute(
                'SELECT * FROM immunities WHERE chat_id IS NULL AND is_active = 1 ORDER BY granted_at DESC'
            )
        return cursor.fetchall()

    def remove_chat_role(self, user_id, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE chat_roles SET is_active = 0 WHERE user_id = ? AND chat_id = ? AND is_active = 1',
            (user_id, chat_id)
        )
        self.conn.commit()

    def get_user_by_nickname(self, nickname, chat_id=None):
        cursor = self.conn.cursor()
        if chat_id:
            cursor.execute(
                'SELECT * FROM chat_nicknames WHERE nickname = ? AND chat_id = ? AND is_active = 1',
                (nickname, chat_id)
            )
        else:
            cursor.execute('SELECT * FROM users WHERE nickname = ?', (nickname,))
        return cursor.fetchone()

    def set_user_nickname(self, user_id, nickname, chat_id, set_by):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO chat_nicknames 
            (user_id, chat_id, nickname, set_by, set_at, is_active)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 1)
        ''', (user_id, chat_id, nickname, set_by))
        self.conn.commit()

    def remove_user_nickname(self, user_id, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE chat_nicknames SET is_active = 0 WHERE user_id = ? AND chat_id = ? AND is_active = 1',
            (user_id, chat_id)
        )
        self.conn.commit()

    def get_user_nickname(self, user_id, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT nickname FROM chat_nicknames WHERE user_id = ? AND chat_id = ? AND is_active = 1',
            (user_id, chat_id)
        )
        result = cursor.fetchone()
        return result['nickname'] if result else None

    def get_top_users(self, limit=10):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT user_id, username, message_count FROM users ORDER BY message_count DESC LIMIT ?',
            (limit,)
        )
        return cursor.fetchall()

    def get_top_users_by_balance(self, limit=10):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT ub.user_id, u.username, ub.balance 
            FROM user_balances ub 
            LEFT JOIN users u ON ub.user_id = u.user_id 
            WHERE ub.balance > 0 
            ORDER BY ub.balance DESC 
            LIMIT ?
        ''', (limit,))
        return cursor.fetchall()

    def increment_message_count(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE users SET message_count = message_count + 1 WHERE user_id = ?',
            (user_id,)
        )
        if cursor.rowcount == 0:
            cursor.execute(
                'INSERT INTO users (user_id, message_count) VALUES (?, 1)',
                (user_id,)
            )
        self.conn.commit()

    def answer_ticket(self, ticket_id, answer, answered_by):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE support_tickets SET status = ?, assigned_to = ?, closed_at = CURRENT_TIMESTAMP WHERE id = ?',
            ('answered', answered_by, ticket_id)
        )
        self.conn.commit()

    def get_all_chat_roles(self, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT cr.*, u.username FROM chat_roles cr LEFT JOIN users u ON cr.user_id = u.user_id WHERE cr.chat_id = ? AND cr.is_active = 1 ORDER BY cr.role_level DESC',
            (chat_id,)
        )
        return cursor.fetchall()

    def get_warn_history(self, user_id, limit=10):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT w.*, u.username as warned_by_name FROM warnings w LEFT JOIN users u ON w.warned_by = u.user_id WHERE w.user_id = ? ORDER BY w.created_at DESC LIMIT ?',
            (user_id, limit)
        )
        return cursor.fetchall()

    def get_all_warnings(self, chat_id=None, limit=20):
        cursor = self.conn.cursor()
        if chat_id:
            cursor.execute(
                'SELECT w.*, u.username as user_name FROM warnings w LEFT JOIN users u ON w.user_id = u.user_id WHERE w.chat_id = ? ORDER BY w.created_at DESC LIMIT ?',
                (chat_id, limit)
            )
        else:
            cursor.execute(
                'SELECT w.*, u.username as user_name FROM warnings w LEFT JOIN users u ON w.user_id = u.user_id ORDER BY w.created_at DESC LIMIT ?',
                (limit,)
            )
        return cursor.fetchall()

    def get_users_with_warnings(self, chat_id):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT 
                w.user_id,
                u.username as user_name,
                COUNT(*) as warning_count
            FROM warnings w
            LEFT JOIN users u ON w.user_id = u.user_id
            WHERE w.chat_id = ?
            GROUP BY w.user_id
            ORDER BY warning_count DESC, w.user_id
        ''', (chat_id,))
        return cursor.fetchall()

    def get_users_with_active_mutes(self, chat_id):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT 
                m.user_id,
                u.username as user_name,
                m.mute_until,
                m.reason
            FROM mutes m
            LEFT JOIN users u ON m.user_id = u.user_id
            WHERE m.chat_id = ? 
            AND m.is_active = 1 
            AND (m.mute_until IS NULL OR m.mute_until > datetime("now"))
            ORDER BY m.mute_until DESC
        ''', (chat_id,))
        return cursor.fetchall()

    def update_user_balance(self, user_id, amount):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT OR IGNORE INTO user_balances (user_id) VALUES (?)',
            (user_id,)
        )
        cursor.execute(
            'UPDATE user_balances SET balance = balance + ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?',
            (amount, user_id)
        )
        self.conn.commit()

    def create_roulette_game(self, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT INTO roulette_games (chat_id) VALUES (?)',
            (chat_id,)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_active_roulette_game(self, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM roulette_games WHERE chat_id = ? AND is_active = 1 ORDER BY created_at DESC LIMIT 1',
            (chat_id,)
        )
        return cursor.fetchone()

    def add_roulette_bet(self, game_id, user_id, username, bet_type, bet_target, bet_amount):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT INTO roulette_bets (game_id, user_id, username, bet_type, bet_target, bet_amount) VALUES (?, ?, ?, ?, ?, ?)',
            (game_id, user_id, username, bet_type, bet_target, bet_amount)
        )
        self.conn.commit()

    def get_game_bets(self, game_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM roulette_bets WHERE game_id = ? ORDER BY created_at ASC',
            (game_id,)
        )
        return cursor.fetchall()

    def end_roulette_game(self, game_id, winning_number):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE roulette_games SET status = ?, ended_at = CURRENT_TIMESTAMP, winning_number = ?, is_active = 0 WHERE id = ?',
            ('ended', winning_number, game_id)
        )
        self.conn.commit()

    def create_crash_game(self, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT INTO crash_games (chat_id) VALUES (?)',
            (chat_id,)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_active_crash_game(self, chat_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM crash_games WHERE chat_id = ? AND is_active = 1 ORDER BY created_at DESC LIMIT 1',
            (chat_id,)
        )
        return cursor.fetchone()

    def add_crash_bet(self, game_id, user_id, username, bet_amount, target_multiplier):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT INTO crash_bets (game_id, user_id, username, bet_amount, target_multiplier) VALUES (?, ?, ?, ?, ?)',
            (game_id, user_id, username, bet_amount, target_multiplier)
        )
        self.conn.commit()

    def get_crash_game_bets(self, game_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM crash_bets WHERE game_id = ? ORDER BY created_at ASC',
            (game_id,)
        )
        return cursor.fetchall()

    def end_crash_game(self, game_id, crash_multiplier):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE crash_games SET status = ?, ended_at = CURRENT_TIMESTAMP, crash_multiplier = ?, is_active = 0 WHERE id = ?',
            ('ended', crash_multiplier, game_id)
        )
        self.conn.commit()

    def transfer_balance(self, sender_id, receiver_id, amount):
        """
        Переводит баланс от отправителя к получателю.
        Возвращает (True, "Сообщение") в случае успеха, (False, "Ошибка") в случае неудачи.
        """
        sender_balance_data = self.get_user_balance(sender_id)
        receiver_balance_data = self.get_user_balance(receiver_id)

        if sender_balance_data['balance'] < amount:
            return False, f"Недостаточно средств! Ваш баланс: {sender_balance_data['balance']:,} $"

        if amount <= 0:
            return False, "Сумма перевода должна быть положительной."

        # Начинаем транзакцию
        self.conn.execute('BEGIN')
        try:
            # Уменьшаем баланс отправителя
            self.update_user_balance(sender_id, -amount)
            # Увеличиваем баланс получателя
            self.update_user_balance(receiver_id, amount)

            self.conn.commit()
            return True, "Перевод успешно выполнен."
        except Exception as e:
            self.conn.execute('ROLLBACK')
            print(f"Ошибка транзакции перевода: {e}")
            return False, "Произошла ошибка при переводе средств. Попробуйте позже."

    def create_dice_game(self, chat_id, creator_id, creator_username, bet_amount, max_players=2):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT INTO dice_games (chat_id, creator_id, creator_username, bet_amount, max_players) VALUES (?, ?, ?, ?, ?)',
            (chat_id, creator_id, creator_username, bet_amount, max_players)
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_active_dice_games(self, chat_id, limit=5):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM dice_games WHERE chat_id = ? AND status = "waiting" AND is_active = 1 ORDER BY created_at ASC LIMIT ?',
            (chat_id, limit)
        )
        return cursor.fetchall()

    def get_dice_game(self, game_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM dice_games WHERE id = ? AND is_active = 1',
            (game_id,)
        )
        return cursor.fetchone()

    def join_dice_game(self, game_id, user_id, username):
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT INTO dice_players (game_id, user_id, username) VALUES (?, ?, ?)',
            (game_id, user_id, username)
        )
        self.conn.commit()

    def get_dice_players(self, game_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM dice_players WHERE game_id = ? ORDER BY joined_at ASC',
            (game_id,)
        )
        return cursor.fetchall()

    def get_dice_players_count(self, game_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT COUNT(*) as count FROM dice_players WHERE game_id = ?',
            (game_id,)
        )
        return cursor.fetchone()['count']

    def is_user_in_dice_game(self, game_id, user_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT COUNT(*) as count FROM dice_players WHERE game_id = ? AND user_id = ?',
            (game_id, user_id)
        )
        return cursor.fetchone()['count'] > 0

    def cancel_dice_game(self, game_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE dice_games SET status = "cancelled", is_active = 0 WHERE id = ?',
            (game_id,)
        )
        self.conn.commit()

    def set_dice_result(self, game_id, user_id, dice_result):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE dice_players SET dice_result = ? WHERE game_id = ? AND user_id = ?',
            (dice_result, game_id, user_id)
        )
        self.conn.commit()

    def end_dice_game(self, game_id, winner_id):
        cursor = self.conn.cursor()
        cursor.execute(
            'UPDATE dice_games SET status = "finished", ended_at = CURRENT_TIMESTAMP, winner_id = ?, is_active = 0 WHERE id = ?',
            (winner_id, game_id)
        )
        self.conn.commit()

    # Методы для работы с зарегистрированными беседами
    def get_registered_chat(self, chat_id):
        """Получить информацию о зарегистрированной беседе"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM registered_chats WHERE chat_id = ?', (chat_id,))
        return cursor.fetchone()

    def is_chat_registered(self, chat_id):
        """Проверить зарегистрирована ли беседа"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT is_registered FROM registered_chats WHERE chat_id = ?', (chat_id,))
        result = cursor.fetchone()
        return result and result['is_registered']

    def register_chat(self, chat_id, registered_by, owner_id, title=None):
        """Зарегистрировать беседу"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO registered_chats 
            (chat_id, is_registered, registered_by, registration_date, owner_id, title, updated_at)
            VALUES (?, 1, ?, CURRENT_TIMESTAMP, ?, ?, CURRENT_TIMESTAMP)
        ''', (chat_id, registered_by, owner_id, title))
        self.conn.commit()

    def unregister_chat(self, chat_id):
        """Отменить регистрацию беседы"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE registered_chats 
            SET is_registered = 0, updated_at = CURRENT_TIMESTAMP 
            WHERE chat_id = ?
        ''', (chat_id,))
        self.conn.commit()

    # Методы для системных администраторов
    def add_system_admin(self, user_id, username, access_level, granted_by):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO system_admins 
            (user_id, username, access_level, granted_by, granted_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (user_id, username, access_level, granted_by))
        self.conn.commit()

    def remove_system_admin(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM system_admins WHERE user_id = ?', (user_id,))
        self.conn.commit()

    def get_all_system_admins(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM system_admins ORDER BY access_level DESC, granted_at ASC')
        return cursor.fetchall()

    def add_system_ban(self, user_id, reason, banned_by, days=None):
        cursor = self.conn.cursor()
        banned_until = None
        if days:
            banned_until = (datetime.now() + timedelta(days=days)).isoformat()

        cursor.execute('''
            INSERT INTO system_bans 
            (user_id, reason, banned_by, banned_until, is_active) 
            VALUES (?, ?, ?, ?, 1)
        ''', (user_id, reason, banned_by, banned_until))
        self.conn.commit()

    def get_system_ban(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM system_bans 
            WHERE user_id = ? AND is_active = 1 
            AND (banned_until IS NULL OR banned_until > datetime("now"))
            ORDER BY created_at DESC LIMIT 1
        ''', (user_id,))
        return cursor.fetchone()

    def remove_system_ban(self, user_id):
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE system_bans 
            SET is_active = 0 
            WHERE user_id = ? AND is_active = 1
        ''', (user_id,))
        self.conn.commit()

    def is_system_banned(self, user_id):
        ban = self.get_system_ban(user_id)
        return ban is not None

    def set_support_chat(self, chat_id):
        """Устанавливает чат для получения репортов"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO bot_settings (key, value, updated_at)
            VALUES ('support_chat_id', ?, CURRENT_TIMESTAMP)
        ''', (str(chat_id),))
        self.conn.commit()

    def get_support_chat(self):
        """Получает ID чата поддержки"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT value FROM bot_settings WHERE key = ?', ('support_chat_id',))
        result = cursor.fetchone()
        return int(result['value']) if result else None

    def get_ticket_by_id(self, ticket_id):
        """Получить тикет по ID"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM support_tickets WHERE id = ?', (ticket_id,))
        return cursor.fetchone()

    def add_filtered_word(self, chat_id, word, added_by):
        """Добавить запрещенное слово в чат"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                'INSERT INTO filtered_words (chat_id, word, added_by) VALUES (?, ?, ?)',
                (chat_id, word.lower(), added_by)
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def remove_filtered_word(self, chat_id, word):
        """Удалить запрещенное слово из чата"""
        cursor = self.conn.cursor()
        cursor.execute(
            'DELETE FROM filtered_words WHERE chat_id = ? AND word = ?',
            (chat_id, word.lower())
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def get_filtered_words(self, chat_id):
        """Получить список всех запрещенных слов для чата"""
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT word FROM filtered_words WHERE chat_id = ? ORDER BY word ASC',
            (chat_id,)
        )
        return [row['word'] for row in cursor.fetchall()]

    def check_message_for_filtered_words(self, chat_id, message_text):
        """Проверить сообщение на наличие запрещенных слов"""
        filtered_words = self.get_filtered_words(chat_id)
        if not filtered_words:
            return None

        message_lower = message_text.lower()
        for word in filtered_words:
            if word in message_lower:
                return word
        return None

    def get_expired_mutes(self):
        """Получить все истекшие муты"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT * FROM mutes 
            WHERE is_active = 1 
            AND mute_until IS NOT NULL 
            AND datetime(mute_until) <= datetime('now')
        ''')
        return cursor.fetchall()

    def remove_mute_by_id(self, mute_id):
        """Снять мут по ID"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE mutes 
            SET is_active = 0 
            WHERE id = ?
        ''', (mute_id,))
        self.conn.commit()

    def close(self):
        self.conn.close()

class VKBot:
    def __init__(self):
        self.token = VK_TOKEN
        self.group_id = VK_GROUP_ID
        self.api_version = CONFIG.get('api_version', '5.131')
        self.server = None
        self.key = None
        self.ts = None
        self.db = Database()
        self.registering_chats = {}
        self.mute_check_timer = None
        self.start_time = time.time()
        self.running = True

        # Инициализировать разработчика
        self.initialize_developer()

        if not self.token or not self.group_id:
            raise ValueError("VK_TOKEN и VK_GROUP_ID должны быть установлены в config.json")

        self.start_mute_checker()
        self.start_mute_expiration_checker()

    def log(self, message):
        """Метод для логирования сообщений"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        print(log_message)

    def run(self):
        """Основной метод для запуска бота"""
        self.log("🤖 Бот запускается...")
        
        if not self.get_long_poll_server():
            self.log("❌ Не удалось запустить бота: ошибка получения Long Poll сервера")
            return

        self.log("✅ Бот успешно запущен!")
        
        # Основной цикл обработки событий
        while self.running:
            try:
                response = requests.get(
                    f"{self.server}?act=a_check&key={self.key}&ts={self.ts}&wait=25"
                ).json()

                if 'failed' in response:
                    if response['failed'] == 1:
                        self.ts = response['ts']
                    elif response['failed'] == 2:
                        self.get_long_poll_server()
                    elif response['failed'] == 3:
                        self.get_long_poll_server()
                    continue

                self.ts = response['ts']
                self.handle_events(response.get('updates', []))
                
            except Exception as e:
                self.log(f"❌ Ошибка в основном цикле: {e}")
                time.sleep(5)

    def handle_events(self, updates):
        """Обработка событий от Long Poll"""
        for update in updates:
            try:
                if update['type'] == 'message_new':
                    self.handle_message(update['object']['message'])
            except Exception as e:
                self.log(f"❌ Ошибка обработки события: {e}")

    def handle_message(self, message):
        """Обработка входящих сообщений"""
        try:
            peer_id = message['peer_id']
            from_id = message['from_id']
            text = message.get('text', '').strip()
            
            # Определяем тип чата
            chat_id = None
            if peer_id > 2000000000:
                chat_id = peer_id - 2000000000

            # Получаем информацию об отправителе
            user_info = self.get_user_info(from_id)
            username = user_info['screen_name'] if user_info else str(from_id)

            # Обработка команд
            if text.startswith('/'):
                self.handle_command(peer_id, from_id, username, text, chat_id, message)
            else:
                # Обработка обычных сообщений
                self.handle_regular_message(peer_id, from_id, username, text, chat_id, message)

        except Exception as e:
            self.log(f"❌ Ошибка обработки сообщения: {e}")

    def handle_command(self, peer_id, user_id, username, text, chat_id, message):
        """Обработка команд"""
        try:
            parts = text.split()
            command = parts[0].lower()
            args = parts[1:] if len(parts) > 1 else []

            # Обработка основных команд
            if command == '/help':
                self.command_help(peer_id)
            elif command == '/ping':
                self.command_ping(peer_id)
            elif command == '/start':
                self.command_start(peer_id, user_id, chat_id)
            elif command == '/rules':
                self.command_rules(peer_id)
            elif command == '/role':
                target_id = self.get_target_user_from_command(message, args)
                role_level = args[1] if len(args) > 1 else None
                self.command_role(peer_id, user_id, target_id, role_level, chat_id)
            elif command == '/ban':
                target_id = self.get_target_user_from_command(message, args)
                reason = ' '.join(args[1:]) if len(args) > 1 else 'Не указана'
                days = None
                if args and args[0].isdigit():
                    days = int(args[0])
                    reason = ' '.join(args[1:]) if len(args) > 1 else 'Не указана'
                self.command_ban(peer_id, user_id, target_id, reason, chat_id, days)
            elif command == '/kick':
                target_id = self.get_target_user_from_command(message, args)
                reason = ' '.join(args[1:]) if len(args) > 1 else 'Не указана'
                self.command_kick(peer_id, user_id, target_id, reason, chat_id)
            elif command == '/mute':
                target_id = self.get_target_user_from_command(message, args)
                duration = int(args[1]) if len(args) > 1 and args[1].isdigit() else 60
                reason = ' '.join(args[2:]) if len(args) > 2 else 'Не указана'
                self.command_mute(peer_id, user_id, target_id, duration, reason, chat_id)
            elif command == '/unmute':
                target_id = self.get_target_user_from_command(message, args)
                self.command_unmute(peer_id, user_id, target_id, chat_id)
            elif command == '/warn':
                target_id = self.get_target_user_from_command(message, args)
                reason = ' '.join(args[1:]) if len(args) > 1 else 'Не указана'
                self.command_warn(peer_id, user_id, target_id, reason, chat_id)
            elif command == '/unwarn':
                target_id = self.get_target_user_from_command(message, args)
                self.command_unwarn(peer_id, user_id, target_id, chat_id)
            elif command == '/staff':
                self.command_staff(peer_id, chat_id)
            elif command == '/balance':
                self.command_balance(peer_id, user_id)
            elif command == '/report':
                report_text = ' '.join(args) if args else ''
                self.command_report(peer_id, user_id, report_text)
            elif command == '/stats':
                target_id = self.get_target_user_from_command(message, args) if args else user_id
                self.command_stats(peer_id, user_id, target_id, chat_id)
            # Добавьте обработку других команд по аналогии

            else:
                # Если команда не распознана
                similar_commands = self.get_similar_commands(command[1:])
                if similar_commands:
                    response = f"❌ Команда {command} не найдена.\n\n💡 Возможно, вы имели в виду:\n" + "\n".join(similar_commands[:3])
                else:
                    response = f"❌ Команда {command} не найдена. Используйте /help для списка команд."
                self.send_message(peer_id, response)

        except Exception as e:
            self.log(f"❌ Ошибка обработки команды {text}: {e}")
            self.send_message(peer_id, "❌ Произошла ошибка при выполнении команды.")

    def handle_regular_message(self, peer_id, user_id, username, text, chat_id, message):
        """Обработка обычных сообщений"""
        try:
            # Увеличиваем счетчик сообщений
            self.db.increment_message_count(user_id)

            # Проверка на запрещенные слова
            if chat_id:
                filtered_word = self.db.check_message_for_filtered_words(chat_id, text)
                if filtered_word:
                    # Удаляем сообщение с запрещенным словом
                    self.delete_message(peer_id, message['conversation_message_id'])
                    warning_msg = f"⚠️ [id{user_id}|{username}], ваше сообщение удалено из-за запрещенного слова: {filtered_word}"
                    self.send_message(peer_id, warning_msg)
                    return

            # Проверка активного мута
            if chat_id:
                active_mute = self.db.get_active_mute_in_chat(user_id, chat_id)
                if active_mute:
                    # Удаляем сообщение от заглушенного пользователя
                    self.delete_message(peer_id, message['conversation_message_id'])
                    
                    # Форматируем дату окончания мута
                    mute_until = active_mute['mute_until']
                    if mute_until:
                        try:
                            mute_dt = datetime.fromisoformat(mute_until.replace('Z', '+00:00'))
                            months = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 
                                     'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
                            mute_until_formatted = f"{mute_dt.day} {months[mute_dt.month - 1]} {mute_dt.year} в {mute_dt.strftime('%H:%M')}"
                        except:
                            mute_until_formatted = mute_until[:16].replace('T', ' ')
                    else:
                        mute_until_formatted = 'Навсегда'

                    mute_msg = f"🔇 [id{user_id}|{username}], вы заглушены до {mute_until_formatted}"
                    self.send_message(peer_id, mute_msg)
                    return

        except Exception as e:
            self.log(f"❌ Ошибка обработки обычного сообщения: {e}")

    def delete_message(self, peer_id, conversation_message_id):
        """Удалить сообщение"""
        try:
            params = {
                'peer_id': peer_id,
                'cmids': conversation_message_id,
                'delete_for_all': 1
            }
            self.api_request('messages.delete', params)
        except Exception as e:
            self.log(f"❌ Ошибка удаления сообщения: {e}")

    def start_mute_checker(self):
        """Запустить периодическую проверку истекших мутов"""
        def check_expired_mutes():
            try:
                # Создаем новое соединение с БД для этого потока
                db = Database()
                expired_mutes = db.get_expired_mutes()
                
                for mute in expired_mutes:
                    try:
                        # Снимаем мут
                        db.remove_mute_by_id(mute['id'])

                        # Отправляем уведомление в чат
                        chat_id = mute['chat_id']
                        user_id = mute['user_id']
                        peer_id = 2000000000 + chat_id

                        # Получаем информацию о пользователе
                        user_info = self.get_user_info(user_id)
                        user_name = user_info['screen_name'] if user_info else str(user_id)

                        message = f"⚠️ У @{user_name}(Пользователя) закончилась блокировка чата."
                        self.send_message(peer_id, message)
                        self.log(f"Автоматически снят мут с пользователя {user_id} в чате {chat_id}")

                    except Exception as e:
                        self.log(f"Ошибка обработки истекшего мута {mute['id']}: {e}")

                db.close()
                
            except Exception as e:
                self.log(f"Ошибка при проверке истекших мутов: {e}")

            # Запускаем следующую проверку через 60 секунд, если бот еще работает
            if self.running:
                self.mute_check_timer = threading.Timer(60.0, check_expired_mutes)
                self.mute_check_timer.daemon = True
                self.mute_check_timer.start()

        # Первая проверка через 10 секунд после запуска
        if self.running:
            self.mute_check_timer = threading.Timer(10.0, check_expired_mutes)
            self.mute_check_timer.daemon = True
            self.mute_check_timer.start()
            self.log("Система автоматической проверки мутов запущена")

    def start_mute_expiration_checker(self):
        """Запускает фоновую проверку истекших блокировок чата"""
        self.check_expired_mutes()
        # Запускаем проверку каждые 60 секунд, если бот еще работает
        if self.running:
            self.mute_check_timer = threading.Timer(60.0, self.start_mute_expiration_checker)
            self.mute_check_timer.daemon = True
            self.mute_check_timer.start()

    def stop(self):
        """Остановка бота"""
        self.log("🛑 Остановка бота...")
        self.running = False
        
        # Останавливаем таймеры
        if self.mute_check_timer:
            self.mute_check_timer.cancel()
            
        # Закрываем соединение с БД
        if self.db:
            self.db.close()
            
        self.log("✅ Бот остановлен")

    # ... остальные методы класса VKBot ...
    def initialize_developer(self):
        try:
            # Проверяем, есть ли разработчик в БД
            existing_dev = self.db.get_system_admin(GRAND_DEVELOPER_ID)
            if not existing_dev:
                # Добавляем разработчика в БД
                self.db.add_system_admin(GRAND_DEVELOPER_ID, GRAND_DEVELOPER_USERNAME, 5, GRAND_DEVELOPER_ID)
                self.log(f"Разработчик инициализирован в БД: {GRAND_DEVELOPER_USERNAME} (ID: {GRAND_DEVELOPER_ID})")
            else:
                self.log(f"Разработчик уже в БД: {GRAND_DEVELOPER_USERNAME}")
        except Exception as e:
            self.log(f"Ошибка инициализации разработчика: {e}")

    def has_permission(self, user_id, username, required_level: Union[str, int] = 'user', chat_id=None):
        global GRAND_MANAGER_ID

        # Гранд чат-менеджер имеет все права
        if GRAND_MANAGER_ID and user_id == GRAND_MANAGER_ID:
            return True

        # Разработчик из конфига имеет все права (если настроено)
        if hasattr(self, 'GRAND_DEVELOPER_ID') and user_id == self.GRAND_DEVELOPER_ID:
            return True

        # Резервная проверка по username
        if not GRAND_MANAGER_ID and username == CONFIG['grand_manager']:
            return True

        try:
            user_role = self.get_user_role(user_id, chat_id)
            if isinstance(required_level, str):
                level_map = {'user': 0, 'moderator': 20, 'admin': 40}
                min_level = level_map.get(required_level, 0)
            else:
                min_level = required_level

            return user_role['level'] >= min_level
        except Exception as e:
            self.log(f"Ошибка проверки прав пользователя: {e}")
            return False

        self.start_mute_checker()
        self.start_mute_expiration_checker()

    def start_mute_expiration_checker(self):
        """Запускает фоновую проверку истекших блокировок чата"""
        self.check_expired_mutes()
        # Запускаем проверку каждые 60 секунд
        self.mute_check_timer = threading.Timer(60.0, self.start_mute_expiration_checker)
        self.mute_check_timer.daemon = True
        self.mute_check_timer.start()

    def check_expired_mutes(self):
        """Проверяет и обрабатывает истекшие блокировки чата"""
        try:
            cursor = self.db.conn.cursor()
            # Находим все активные блокировки, у которых истек срок
            cursor.execute('''
                SELECT id, user_id, chat_id, mute_until 
                FROM mutes 
                WHERE is_active = 1 
                AND mute_until IS NOT NULL
                AND mute_until <= datetime("now")
            ''')
            expired_mutes = cursor.fetchall()

            for mute in expired_mutes:
                try:
                    # Снимаем мут
                    self.db.remove_mute_by_id(mute['id'])

                    # Отправляем уведомление в чат
                    # Исправляем формирование peer_id для групповых чатов
                    if mute['chat_id']:
                        chat_peer_id = 2000000000 + mute['chat_id']
                    else:
                        chat_peer_id = mute['user_id']

                    # Получаем отображаемое имя пользователя (никнейм или screen_name)
                    display_name = self.get_display_name(mute['user_id'], mute['chat_id'])

                    # Получаем информацию о пользователе для упоминания
                    user_info = self.get_user_info(mute['user_id'])
                    if user_info:
                        first_name = user_info.get('first_name', display_name)
                        message = f"⚠ У [id{mute['user_id']}|{first_name}] закончилась блокировка чата."
                    else:
                        message = f"⚠ У [id{mute['user_id']}|Пользователя] закончилась блокировка чата."

                    self.send_message(chat_peer_id, message)
                    self.log(f"Блокировка истекла для пользователя {mute['user_id']} в чате {mute['chat_id']}")

                except Exception as e:
                    self.log(f"Ошибка обработки истекшей блокировки {mute['id']}: {e}")

        except Exception as e:
            self.log(f"Ошибка проверки истекших блокировок: {e}")

    def get_role_name_for_level(self, role_level, chat_id):
        """Получает название роли для указанного уровня с учетом кастомных ролей чата"""
        if role_level == 0:
            return 'Пользователь'

        if chat_id:
            try:
                cursor = self.db.conn.cursor()
                cursor.execute(
                    'SELECT role_name FROM custom_role_definitions WHERE chat_id = ? AND role_level = ? AND is_active = 1',
                    (chat_id, role_level)
                )
                custom_role = cursor.fetchone()
                if custom_role:
                    return custom_role['role_name']
            except Exception as e:
                self.log(f"Ошибка получения кастомного названия роли: {e}")

        return CONFIG['roles'].get(role_level, f'Роль {role_level}')

    def role_exists(self, role_level, chat_id):
        """Проверяет, существует ли роль с указанным уровнем"""
        if role_level == 0:
            return True

        if role_level in CONFIG['roles']:
            return True

        if chat_id:
            try:
                cursor = self.db.conn.cursor()
                cursor.execute(
                    'SELECT COUNT(*) as count FROM custom_role_definitions WHERE chat_id = ? AND role_level = ? AND is_active = 1',
                    (chat_id, role_level)
                )
                result = cursor.fetchone()
                return result['count'] > 0
            except Exception as e:
                self.log(f"Ошибка проверки существования роли: {e}")

        return False

    def get_similar_commands(self, entered_command):
        """Находит похожие команды по первой букве"""
        if not entered_command:
            return []

        first_letter = entered_command[0].lower()
        similar_commands = []

        for cmd_key, cmd_aliases in CONFIG['commands'].items():
            for alias in cmd_aliases:
                if alias.lower().startswith(first_letter) and alias.lower() != entered_command.lower():
                    similar_commands.append('/' + alias)

        return list(set(similar_commands))[:5]

    def api_request(self, method, params=None):
        if params is None:
            params = {}

        params['access_token'] = self.token
        params['v'] = self.api_version

        url = f'https://api.vk.com/method/{method}'

        try:
            response = requests.post(url, data=params)
            result = response.json()

            if 'error' in result:
                self.log(f"VK API Error: {result['error']}")
                return None

            return result.get('response')
        except Exception as e:
            self.log(f"Ошибка API запроса: {e}")
            return None

    def get_long_poll_server(self):
        response = self.api_request('groups.getLongPollServer', {
            'group_id': self.group_id
        })

        if response:
            self.server = response['server']
            self.key = response['key']
            self.ts = response['ts']
            self.log(f"Long Poll сервер настроен: {self.server[:50]}...")
            return True
        else:
            self.log("❌ Не удалось получить Long Poll сервер. Проверьте настройки группы ВК!")
        return False

    def extract_user_id(self, mention):
        if not mention:
            return None

        # Формат [id123456789|Имя Фамилия]
        id_match = re.search(r'\[id(\d+)\|.*?\]', mention)
        if id_match:
            return int(id_match.group(1))

        # Формат @username или @id123456789
        at_match = re.search(r'^@?(.+)$', mention)
        if at_match:
            username = at_match.group(1)
            if username.isdigit():
                return int(username)
            return username

        return None

    def resolve_user_id(self, mention):
        extracted = self.extract_user_id(mention)
        if not extracted:
            return None

        if isinstance(extracted, int):
            return extracted

        # Разрешаем username через API
        response = self.api_request('utils.resolveScreenName', {
            'screen_name': extracted
        })

        if response and response.get('type') == 'user':
            return response['object_id']

        return None

    def get_target_user_from_command(self, message, args, arg_index=1):
        # Проверяем ответ на сообщение
        if message.get('reply_message') and message['reply_message'].get('from_id'):
            return message['reply_message']['from_id']

        # Проверяем аргумент команды
        if len(args) > arg_index:
            return self.resolve_user_id(args[arg_index])

        return None

    def get_user_info(self, user_id):
        response = self.api_request('users.get', {
            'user_ids': user_id,
            'fields': 'screen_name'
        })

        if response and len(response) > 0:
            return response[0]
        return None

def has_permission(self, user_id, username, required_level: Union[str, int] = 'user', chat_id=None):
    global GRAND_MANAGER_ID

    # Гранд чат-менеджер имеет все права
    if GRAND_MANAGER_ID and user_id == GRAND_MANAGER_ID:
        return True

    # Разработчик из конфига имеет все права (если настроено)
    if hasattr(self, 'GRAND_DEVELOPER_ID') and user_id == self.GRAND_DEVELOPER_ID:
        return True

    # Резервная проверка по username
    if not GRAND_MANAGER_ID and username == CONFIG['grand_manager']:
        return True

    try:
        user_role = self.get_user_role(user_id, chat_id)
        if isinstance(required_level, str):
            level_map = {'user': 0, 'moderator': 20, 'admin': 40}
            min_level = level_map.get(required_level, 0)
        else:
            min_level = required_level

        return user_role['level'] >= min_level
    except Exception as e:
        self.log(f"Ошибка проверки прав пользователя: {e}")
        return False

def initialize_developer(self):
    try:
        # Проверяем, есть ли разработчик в БД
        existing_dev = self.db.get_system_admin(GRAND_DEVELOPER_ID)
        if not existing_dev:
            # Добавляем разработчика в БД
            self.db.add_system_admin(GRAND_DEVELOPER_ID, GRAND_DEVELOPER_USERNAME, 5, GRAND_DEVELOPER_ID)
            self.log(f"Разработчик инициализирован в БД: {GRAND_DEVELOPER_USERNAME} (ID: {GRAND_DEVELOPER_ID})")
        else:
            self.log(f"Разработчик уже в БД: {GRAND_DEVELOPER_USERNAME}")
    except Exception as e:
        self.log(f"Ошибка инициализации разработчика: {e}")

    def get_user_role(self, user_id, chat_id=None):
        global GRAND_MANAGER_ID

        if GRAND_MANAGER_ID and user_id == GRAND_MANAGER_ID:
            # Проверяем, есть ли кастомное название для роли 100 в этом чате
            if chat_id:
                try:
                    cursor = self.db.conn.cursor()
                    cursor.execute(
                        'SELECT role_name FROM custom_role_definitions WHERE chat_id = ? AND role_level = 100 AND is_active = 1',
                        (chat_id,)
                    )
                    custom_role = cursor.fetchone()
                    if custom_role:
                        return {'level': 100, 'name': custom_role['role_name']}
                except Exception as e:
                    self.log(f"Ошибка получения кастомной роли 100: {e}")
            return {'level': 100, 'name': 'Владелец'}

        try:
            # Проверяем роль в конкретном чате
            if chat_id:
                chat_role = self.db.get_chat_role(user_id, chat_id)
                if chat_role and chat_role['role_level'] > 0:
                    # Используем название из chat_roles (которое уже обновлено)
                    role_name = chat_role['role_name']

                    # Дополнительно проверяем кастомное определение роли
                    cursor = self.db.conn.cursor()
                    cursor.execute(
                        'SELECT role_name FROM custom_role_definitions WHERE chat_id = ? AND role_level = ? AND is_active = 1',
                        (chat_id, chat_role['role_level'])
                    )
                    custom_role = cursor.fetchone()
                    if custom_role:
                        role_name = custom_role['role_name']

                    return {
                        'level': chat_role['role_level'],
                        'name': role_name,
                        'is_chat_specific': True
                    }

            # Проверяем глобальную роль
            user = self.db.get_user(user_id)
            role_level = user['role_level'] if user else 0
            role_name = CONFIG['roles'].get(role_level, 'Пользователь')

            return {
                'level': role_level,
                'name': role_name,
                'is_chat_specific': False
            }
        except Exception as e:
            self.log(f"Ошибка получения роли пользователя: {e}")
            return {'level': 0, 'name': 'Пользователь', 'is_chat_specific': False}

    def can_moderate_user(self, actor_id, target_id, chat_id=None):
        global GRAND_MANAGER_ID

        if actor_id == target_id:
            return {'can_moderate': False, 'reason': '⛔ Доступ запрещён! Вы не можете использовать модерационное действие на себе.'}

        if GRAND_MANAGER_ID and target_id == GRAND_MANAGER_ID:
            return {'can_moderate': False, 'reason': '❌ Нельзя применить модерационное действие к создателю системы.'}

        # Проверяем иммунитет
        try:
            immunity = self.db.get_immunity(target_id, chat_id)
            if immunity:
                return {'can_moderate': False, 'reason': '❌ Пользователь имеет иммунитет от наказаний в этом чате.'}
        except Exception as e:
            self.log(f"Ошибка проверки иммунитета: {e}")

        try:
            actor_role = self.get_user_role(actor_id, chat_id)
            target_role = self.get_user_role(target_id, chat_id)

            if actor_role['level'] < target_role['level']:
                return {
                    'can_moderate': False,
                    'reason': f"⛔️ Отказано! {actor_role['name']} не может применить модерационное действие к {target_role['name']}."
                }

            if actor_role['level'] == target_role['level'] and actor_role['level'] >= 10:
                return {
                    'can_moderate': False,
                    'reason': '⛔ Доступ запрещён! Вы не можете использовать команды на пользователе с равной или более высокой ролью.'
                }

            return {'can_moderate': True}
        except Exception as e:
            self.log(f"Ошибка проверки иерархии ролей: {e}")
            return {'can_moderate': False, 'reason': '❌ Ошибка проверки прав.'}

    def send_message(self, peer_id, message, keyboard=None):
        params = {
            'peer_id': peer_id,
            'message': message,
            'random_id': random.randint(1, 1000000)
        }
        if keyboard:
            params['keyboard'] = keyboard

        response = self.api_request('messages.send', params)
        return response

    def create_dice_keyboard(self, game_id, is_creator=False):
        """Создает клавиатуру для игры в кости"""
        buttons = []

        if not is_creator:
            buttons.append([{
                "action": {
                    "type": "text",
                    "payload": f'{{"action": "join_dice", "game_id": {game_id}}}',
                    "label": "🎲 Играть"
                },
                "color": "positive"
            }])

        if is_creator:
            buttons.append([{
                "action": {
                    "type": "text",
                    "payload": f'{{"action": "cancel_dice", "game_id": {game_id}}}',
                    "label": "❌ Отменить"
                },
                "color": "negative"
            }])

        return json.dumps({
            "one_time": False,
            "buttons": buttons
        })

    def kick_user(self, chat_id, user_id, reason='Не указано'):
        try:
            params = {'chat_id': chat_id}
            if user_id > 0:
                params['user_id'] = user_id
            else:
                params['member_id'] = user_id

            response = self.api_request('messages.removeChatUser', params)
            return response is not None
        except Exception as e:
            self.log(f"Ошибка кика пользователя: {e}")
            return False

    # Команды бота
    def command_help(self, peer_id):
        help_text = """♛ ROLEX 2.0 — удобный админ-бот для модерации.

 ♛ Самые основные команды:
🔰 /help - Основа
🔰 /role - Назначить роль
🔰 /ban - Блокировка
🔰 /unban - Разблокирововка
🔰 /kick - Исключение
🔰 /mute - Выдать mute
🔰 /unmute - Снять mute
🔰 /warn - Выдать warn
🔰 /unwarn - Снять warn
🔰 /snick - Установить никнейм
🔰 /правила - Показать правила 
🔰 /staff - Список админов
🔰 /pin - Закрепить сообщение
🔰 /unpin - Открепить сообщение
🔰 /zov - Упоминание всех

 ✄ Статья команд: https://vk.com/@rolexban-bez-nazvaniya
"""
        self.send_message(peer_id, help_text)

    def command_ahelp(self, peer_id, user_id):
        """Помощь по системным командам администрирования"""
        # Проверяем уровень доступа пользователя
        system_admin = self.db.get_system_admin(user_id)
        if not system_admin:
            self.send_message(peer_id, '❌ У вас нет прав системного администратора.')
            return

        access_level = system_admin['access_level']

        help_text = "🛡️ ПОМОЩЬ ПО АДМИНИСТРАЦИИ БОТА\n\n"

        # Команды агента поддержки (уровень 1+)
        if access_level >= 1:
            help_text += """🎫 КОМАНДЫ АГЕНТА ПОДДЕРЖКИ (1+)
• /ahelp — эта команда
• /sysadmins - список администрации бота
• /answer [айди репорта] ответ - ответить на репорт
• /tickets - все тикеты

"""

        # Команды администратора бота (уровень 2+)
        if access_level >= 2:
            help_text += """⚔️ КОМАНДЫ АДМИНИСТРАТОРА БОТА (2+)
• /giveagent [@user] — выдать права агента поддержки
• /null [@user] — снять права агента/администратора
• /sysban [@user] [дни] [причина] — системный бан
• /unsysban [@user] — снять системный бан
• /sysrole [@user] [уровень] — выдать любую роль которая есть в /роли

"""

        # Команды зам.основателя (уровень 3)
        if access_level >= 3:
            help_text += """👑 КОМАНДЫ ЗАМ.ОСНОВАТЕЛЯ БОТА (3)
• /giveadm [@user] — выдать права администратора бота

"""

        # Команды основателя (уровень 4)
        if access_level >= 4:
            help_text += """👑 КОМАНДЫ ОСНОВАТЕЛЯ БОТА (4)
• /givezam [@user] — выдать Зам.Основателя
• !установить_чат — установить чат для получения репортов

"""

        # Команды разработчика (уровень 5)
        if access_level >= 5:
            help_text += """💻 КОМАНДЫ РАЗРАБОТЧИКА БОТА (5)
• /giveowner [@user] — выдать права основателя бота

"""

        self.send_message(peer_id, help_text)

    def command_ping(self, peer_id):
        import time as time_module

        # Измеряем время отклика API
        start_time = time_module.time()
        try:
            # Делаем минимальный запрос к API для проверки
            test_response = self.api_request('users.get', {'user_ids': 1})
            api_time = int((time_module.time() - start_time) * 1000)
            network_status = "🟢 Хорошее" if api_time < 500 else ("🟡 Среднее" if api_time < 1500 else "🔴 Слабое")
        except:
            api_time = 9999
            network_status = "🔴 Слабое"

        # Время обработки (фиксированное)
        process_time = 1

        # Вычисляем аптайм бота (если есть время запуска)
        if hasattr(self, 'start_time'):
            uptime_seconds = int(time_module.time() - self.start_time)
            hours = uptime_seconds // 3600
            minutes = (uptime_seconds % 3600) // 60
            seconds = uptime_seconds % 60
            uptime_str = f"{hours}ч {minutes}м {seconds}с"
        else:
            uptime_str = "Неизвестно"

        ping_text = f"""🔅 Состояние системы

• Сеть: {network_status}
• Время отклика: {api_time} мс
• Процессинг: {process_time} мс
• Аптайм: {uptime_str}"""

        self.send_message(peer_id, ping_text)

    def command_start(self, peer_id, user_id=None, chat_id=None):
        # Если это личные сообщения
        if not chat_id:
            welcome_text = """🚀 Привет!

🤖 VK Чат-Менеджер активирован!

• /help 
• Удачи!"""
            self.send_message(peer_id, welcome_text)
            return

        # Если беседа уже зарегистрирована
        if self.is_chat_registered(chat_id):
            welcome_text = """🚀 Беседа уже активна'"""
            self.send_message(peer_id, welcome_text)
            return

        # Защита от двойного нажатия
        if chat_id in self.registering_chats:
            return

        self.registering_chats[chat_id] = True

        # Проверяем права пользователя
        admin_rights = self.check_user_admin_rights(user_id, chat_id)
        if not admin_rights['is_admin'] and not admin_rights['is_owner']:
            error_message = """❌ У вас недостаточно прав!

Активировать бота может:
• Создатель беседы
• Администратор беседы 

💡 Попросите администратора активировать бота"""

            self.send_message(peer_id, error_message)
            return

        # Регистрируем беседу
        try:
            # Получаем информацию о беседе
            chat_info = self.api_request('messages.getConversationsById', {
                'peer_ids': peer_id
            })

            chat_title = "Неизвестная беседа"
            if chat_info and chat_info.get('items'):
                chat_title = chat_info['items'][0].get('chat_settings', {}).get('title', 'Неизвестная беседа')

            # Получаем информацию о участниках беседы
            conversation_info = self.api_request('messages.getConversationMembers', {
                'peer_id': peer_id
            })

            # Определяем владельца
            owner_id = user_id if admin_rights['is_owner'] else None
            if not owner_id and conversation_info and 'items' in conversation_info:
                # Ищем владельца среди участников (только пользователи, не группы)
                for member in conversation_info['items']:
                    member_id = member.get('member_id')
                    if member.get('is_owner') and member_id and member_id > 0:
                        owner_id = member_id
                        break

            # Регистрируем беседу
            self.db.register_chat(chat_id, user_id, owner_id or user_id, chat_title)

            # Назначаем роли
            self.assign_initial_roles(chat_id, conversation_info)

            # Генерируем случайный код чата (в стиле как на примере)
            import string
            chat_code = ''.join(random.choices(string.ascii_lowercase + string.digits, k=11))

            success_message = f"""👍 Прекрасно! Теперь я имею доступ к вашей беседе. Если вас не устраивают стандартные настройки, исправьте это командой /settings

✨ Код вашего чата: {chat_code}"""

            # Отправляем сообщение с фото
            params = {
                'peer_id': peer_id,
                'message': success_message,
                'attachment': 'photo-233140032_457239056',
                'random_id': random.randint(1, 1000000)
            }

            self.api_request('messages.send', params)
            self.log(f"Беседа {chat_id} ({chat_title}) зарегистрирована пользователем {user_id}")

            # Отправляем уведомление в чат поддержки
            support_chat_id = self.db.get_support_chat()
            if support_chat_id:
                user_info = self.get_user_info(user_id)
                user_name = user_info['screen_name'] if user_info else str(user_id)

                admin_notification = f"🔮 @{user_name} активировал бота в беседе: {chat_id}"
                self.send_message(support_chat_id, admin_notification)

        except Exception as e:
            self.log(f"Ошибка регистрации беседы: {e}")
            self.send_message(peer_id, '❌ Ошибка при регистрации беседы. Попробуйте позже.')
        finally:
            # Очищаем флаг регистрации
            if chat_id in self.registering_chats:
                del self.registering_chats[chat_id]

    def handle_bot_invited_to_chat(self, peer_id, chat_id):
        """Обработка добавления бота в беседу"""
        try:
            # Отправляем приветственное сообщение
            welcome_message = "💜 Благодарю за добавление. Выдайте мне пожалуйста права администратора в данной беседе, после чего, нажмите на кнопку \"Активировать\" ниже."

            # Создаем inline-кнопку активации (фиолетовая)
            keyboard = {
                "inline": True,
                "buttons": [
                    [{
                        "action": {
                            "type": "callback",
                            "label": "Активировать",
                            "payload": json.dumps({"action": "activate_chat"})
                        },
                        "color": "primary"
                    }]
                ]
            }

            # Отправляем сообщение с фото
            params = {
                'peer_id': peer_id,
                'message': welcome_message,
                'attachment': 'photo-233140032_457239054',
                'keyboard': json.dumps(keyboard),
                'random_id': random.randint(1, 1000000)
            }

            self.api_request('messages.send', params)
            self.log(f"Бот добавлен в беседу {chat_id}")

        except Exception as e:
            self.log(f"Ошибка при обработке добавления в беседу: {e}")
            # Отправляем упрощенное сообщение если API недоступен
            simple_message = "💜 Благодарю за добавление. Выдайте мне пожалуйста права администратора в данной беседе, после чего используйте команду /start для активации."
            self.send_message(peer_id, simple_message)

    def check_user_ban_on_invite(self, peer_id, chat_id, invited_user_id):
        """Проверка бана пользователя при приглашении в беседу"""
        try:
            # Проверяем, есть ли у пользователя активный бан в этом чате
            user_ban = self.db.get_user_ban_in_chat(invited_user_id, chat_id)

            if user_ban:
                # Получаем информацию о забаненном пользователе
                user_info = self.get_user_info(invited_user_id)
                user_name = user_info['screen_name'] if user_info else str(invited_user_id)

                # Получаем информацию о том, кто забанил
                banned_by_info = self.get_user_info(user_ban['banned_by'])
                banned_by_name = banned_by_info['screen_name'] if banned_by_info else str(user_ban['banned_by'])

                # Формируем сообщение о бане
                ban_message = f"""🚫 Пользователь заблокирован в этом чате!

@{user_name}(Пользователь).

Причина: {user_ban['reason']}
Заблокировал: @{banned_by_name}
Дата бана: {user_ban['created_at'][:10]}"""

                # Отправляем сообщение
                self.send_message(peer_id, ban_message)

                # Кикаем пользователя
                self.kick_user(chat_id, invited_user_id, f"Пользователь в бане: {user_ban['reason']}")

                self.log(f"Забаненный пользователь {user_name} автоматически исключен из чата {chat_id}")

        except Exception as e:
            self.log(f"Ошибка проверки бана при приглашении: {e}")

    def is_chat_registered(self, chat_id):
        """Проверить зарегистрирована ли беседа"""
        if not chat_id:
            return True  # Личные сообщения всегда "зарегистрированы"
        return self.db.is_chat_registered(chat_id)

    def check_user_admin_rights(self, user_id, chat_id):
        """Проверить права администратора пользователя в беседе через VK API"""
        try:
            # Получаем информацию о беседе и участниках
            conversation_info = self.api_request('messages.getConversationMembers', {
                'peer_id': 2000000000 + chat_id
            })

            if not conversation_info or 'items' not in conversation_info:
                return {'is_admin': False, 'is_owner': False}

            # Ищем пользователя среди участников
            for member in conversation_info['items']:
                if member.get('member_id') == user_id:
                    is_owner = member.get('is_owner', False)
                    is_admin = member.get('is_admin', False)

                    return {
                        'is_admin': is_admin or is_owner,
                        'is_owner': is_owner
                    }

            return {'is_admin': False, 'is_owner': False}

        except Exception as e:
            self.log(f"Ошибка проверки прав администратора: {e}")
            return {'is_admin': False, 'is_owner': False}

    def assign_initial_roles(self, chat_id, conversation_info):
        """Назначить начальные роли владельцу и администраторам при регистрации беседы"""
        try:
            if not conversation_info or 'items' not in conversation_info:
                self.log(f"Не удалось получить информацию об участниках беседы {chat_id}")
                return

            for member in conversation_info['items']:
                member_id = member.get('member_id')
                if not member_id or member_id < 0:  # Пропускаем группы/сообщества (отрицательные ID)
                    continue

                # Назначаем роль владельцу
                if member.get('is_owner'):
                    owner_role_name = self.get_role_name_for_level(100, chat_id)
                    self.db.set_chat_role(member_id, chat_id, 100, owner_role_name, member_id)
                    self.log(f"Назначена роль '{owner_role_name}' пользователю {member_id} в беседе {chat_id}")

                # Назначаем роль администраторам
                elif member.get('is_admin'):
                    admin_role_name = self.get_role_name_for_level(80, chat_id)
                    self.db.set_chat_role(member_id, chat_id, 80, admin_role_name, member_id)
                    self.log(f"Назначена роль '{admin_role_name}' пользователю {member_id} в беседе {chat_id}")

        except Exception as e:
            self.log(f"Ошибка назначения начальных ролей в беседе {chat_id}: {e}")

    def command_who(self, peer_id, chat_id, question):
        """Выбирает случайного участника беседы"""
        if not chat_id:
            self.send_message(peer_id, '❌ Эта команда доступна только в беседах!')
            return

        if not question:
            self.send_message(peer_id, '❌ Укажите вопрос! Использование: /кто [вопрос]')
            return

        try:
            # Получаем список участников беседы
            conversation_info = self.api_request('messages.getConversationMembers', {
                'peer_id': 2000000000 + chat_id
            })

            if not conversation_info or 'items' not in conversation_info:
                self.send_message(peer_id, '❌ Не удалось получить список участников беседы.')
                return

            # Фильтруем только реальных пользователей (не группы)
            members = [m for m in conversation_info['items'] if m.get('member_id', 0) > 0]

            if not members:
                self.send_message(peer_id, '❌ В беседе нет участников.')
                return

            # Выбираем случайного участника
            import random
            random_member = random.choice(members)
            member_id = random_member['member_id']

            # Получаем информацию о пользователе
            user_info = self.get_user_info(member_id)
            user_name = user_info['screen_name'] if user_info else str(member_id)

            # Формируем ответ
            response = f"🔮 Мне кажется {question} тут @{user_name}"
            self.send_message(peer_id, response)

        except Exception as e:
            self.log(f"Ошибка команды /кто: {e}")
            self.send_message(peer_id, '❌ Произошла ошибка при выборе участника.')

    def command_rules(self, peer_id):
        rules_text = """📜 Правила чата!

1️⃣ Уважайте других участников
2️⃣ Не спамьте и не флудите
3️⃣ Запрещены оскорбления
4️⃣ Не рекламируйте без разрешения
5️⃣ Используйте команды по назначению
6️⃣ Слушайтесь администрацию

⚠️ Нарушение правил карается предупреждениями,
мутом или баном по решению администрации"""
        self.send_message(peer_id, rules_text)

    def command_roles(self, peer_id, chat_id):
        roles_text = """📋 Список доступных ролей (в скобках указан приоритет):

"""

        # Стандартные роли с новой структурой
        standard_roles = {
            0: 'Пользователь',
            20: 'Помощник',
            40: 'Модератор',
            60: 'Администратор',
            80: 'Спец.Администратор',
            100: 'Создатель'
        }

        # Собираем все роли (стандартные + кастомные)
        all_roles = {}

        # Добавляем стандартные роли
        for level, name in standard_roles.items():
            all_roles[level] = name

        # Добавляем/перезаписываем кастомные роли из чата
        if chat_id:
            try:
                # Получаем кастомные роли для конкретного чата
                custom_roles = self.db.get_chat_custom_roles(chat_id)

                # Добавляем или перезаписываем роли (включая стандартные)
                for role in custom_roles:
                    level = role['role_level']
                    name = role['role_name']
                    # Перезаписываем название роли (даже если это стандартная роль)
                    all_roles[level] = name

            except Exception as e:
                self.log(f"Ошибка получения кастомных ролей: {e}")

        # Выводим все роли в порядке убывания приоритета
        counter = 1
        for level in sorted(all_roles.keys(), reverse=True):
            name = all_roles[level]
            roles_text += f"{counter}. '{name}' ({level})\n"
            counter += 1

        # Добавляем информацию о создании кастомных ролей
        if chat_id:
            roles_text += "\n💡 Кастомные роли создаются командой /newrole"
        else:
            roles_text += "\n❌ Кастомные роли доступны только в групповых чатах"

        self.send_message(peer_id, roles_text)

    def command_try(self, peer_id, action):
        if not action:
            self.send_message(peer_id, 'Ошибка! Использование: /попытка [действие]')
            return

        chances = [
            'Удача! 🍀 Попытка удалась!',
            'Почти получилось! 😅 Попробуйте еще раз!',
            'Неудача... 😔 Но не сдавайтесь!',
            'Великолепно! ⭐ Потрясающий результат!',
            'Средненько... 🤷‍♂️ Бывает и лучше',
            'Фиаско! 💥 Полный провал!',
            'Отлично! 👍 Всё получилось как надо!',
            'Так себе... 😐 Можно было и лучше'
        ]

        result = random.choice(chances)
        self.send_message(peer_id, f'🎲 Попытка "{action}": {result}')

    def command_kiss(self, peer_id, sender_id, target_id):
        if not target_id:
            self.send_message(peer_id, '❌ Использовать /поцеловать [айди/ответ] ')
            return

        if target_id == sender_id:
            self.send_message(peer_id, '😅 Нельзя поцеловать самого себя!')
            return

        sender_info = self.get_user_info(sender_id)
        target_info = self.get_user_info(target_id)

        sender_name = sender_info['screen_name'] if sender_info else str(sender_id)
        target_name = target_info['screen_name'] if target_info else str(target_id)

        messages = [
            f'💋 @{sender_name} нежно поцеловал(а) @{target_name}',
            f'😘 @{sender_name} страстно поцеловал(а) @{target_name}',
            f'💕 @{sender_name} робко поцеловал(а) @{target_name}',
            f'😍 @{sender_name} романтично поцеловал(а) @{target_name}'
        ]

        message = random.choice(messages)
        self.send_message(peer_id, message)

    def command_hug(self, peer_id, sender_id, target_id):
        if not target_id:
            self.send_message(peer_id, '❌ Ошибка! Использование: /обнять (упоминание) или ответьте на сообщение')
            return

        if target_id == sender_id:
            self.send_message(peer_id, '🤗 Обнимаете сами себя... Это мило!')
            return

        sender_info = self.get_user_info(sender_id)
        target_info = self.get_user_info(target_id)

        sender_name = sender_info['screen_name'] if sender_info else str(sender_id)
        target_name = target_info['screen_name'] if target_info else str(target_id)

        messages = [
            f'🤗 @{sender_name} тепло обнял(а) @{target_name}',
            f'🫂 @{sender_name} крепко обнял(а) @{target_name}',
            f'💞 @{sender_name} нежно обнял(а) @{target_name}',
            f'☺️ @{sender_name} дружески обнял(а) @{target_name}'
        ]

        message = random.choice(messages)
        self.send_message(peer_id, message)

    def command_marry(self, peer_id, sender_id, target_id):
        if not target_id:
            self.send_message(peer_id, '❌ Использование: /брак [ID] или ответьте на сообщение')
            return

        if target_id == sender_id:
            self.send_message(peer_id, '💍 Женитьба на самом себе? Оригинально! 😄')
            return

        sender_info = self.get_user_info(sender_id)
        target_info = self.get_user_info(target_id)

        sender_name = sender_info['screen_name'] if sender_info else str(sender_id)
        target_name = target_info['screen_name'] if target_info else str(target_id)

        responses = [
            f'💍 @{sender_name} сделал(а) предложение @{target_name}! Согласится ли?',
            f'💒 @{sender_name} просит руки @{target_name}! Какой романтик!',
            f'💕 @{sender_name} хочет жениться на @{target_name}! Свадьба будет?',
            f'🤵‍♂️👰‍♀️ @{sender_name} и @{target_name} - прекрасная пара!'
        ]

        response = random.choice(responses)
        self.send_message(peer_id, response)

    def command_divorce(self, peer_id, sender_id):
        sender_info = self.get_user_info(sender_id)
        sender_name = sender_info['screen_name'] if sender_info else str(sender_id)

        responses = [
            f'💔 @{sender_name} подал(а) на развод... Грустно!',
            f'😢 @{sender_name} решил(а) развестись. Жаль...',
            f'💸 @{sender_name} теперь свободен(на)! Алименты не забудьте!',
            f'🎉 @{sender_name} празднует развод! Свобода!'
        ]

        response = random.choice(responses)
        self.send_message(peer_id, response)

    def command_rape(self, peer_id, sender_id, target_id):
        if not target_id:
            self.send_message(peer_id, '❌ Использование: /надругаться [ID/ссылка] или ответьте на сообщение')
            return

        if target_id == sender_id:
            self.send_message(peer_id, '😅 Надругаться над самим собой? Это что-то новенькое!')
            return

        sender_info = self.get_user_info(sender_id)
        target_info = self.get_user_info(target_id)

        sender_name = sender_info['screen_name'] if sender_info else str(sender_id)
        target_name = target_info['screen_name'] if target_info else str(target_id)

        messages = [
            f'😈 @{sender_name} надругался(лась) над @{target_name}',
            f'🔞 @{sender_name} изнасиловал(а) @{target_name}',
            f'💀 @{sender_name} жестоко надругался(лась) над @{target_name}',
            f'😱 @{sender_name} безжалостно изнасиловал(а) @{target_name}'
        ]

        message = random.choice(messages)
        self.send_message(peer_id, message)

    def command_oral(self, peer_id, sender_id, target_id):
        if not target_id:
            self.send_message(peer_id, '❌ Использование: /отсосать [ID/ссылка] или ответьте на сообщение')
            return

        if target_id == sender_id:
            self.send_message(peer_id, '😅 Отсосать самому себе? Гибкий вы человек!')
            return

        sender_info = self.get_user_info(sender_id)
        target_info = self.get_user_info(target_id)

        sender_name = sender_info['screen_name'] if sender_info else str(sender_id)
        target_name = target_info['screen_name'] if target_info else str(target_id)

        messages = [
            f'🔞 @{sender_name} отсосал(а) у @{target_name}',
            f'😏 @{sender_name} сделал(а) приятно @{target_name}',
            f'💦 @{sender_name} умело отработал(а) ртом у @{target_name}',
            f'👄 @{sender_name} отлично поработал(а) ртом с @{target_name}'
        ]

        message = random.choice(messages)
        self.send_message(peer_id, message)

    def command_warn(self, peer_id, sender_id, target_id, reason, chat_id):
        # Проверка на отсутствие пользователя
        if not target_id:
            error_message = """☕️ Отказано! Необходимо указать пользователя и причину.

☕️ Примеры использования:
/warn @user 1.2
/warn @user 1.1"""
            self.send_message(peer_id, error_message)
            return

        # Если причина не указана, используем стандартную
        if not reason or not reason.strip():
            reason = "Нарушение правил"

        moderation_check = self.can_moderate_user(sender_id, target_id, chat_id)
        if not moderation_check['can_moderate']:
            self.send_message(peer_id, moderation_check['reason'])
            return

        try:
            self.db.add_warning(target_id, reason, sender_id, chat_id)
            warnings = self.db.get_user_warnings(target_id)
            warning_count = len(warnings)

            target_display = self.get_display_name(target_id, chat_id)

            if warning_count >= 3:
                # При достижении 3 предупреждений - кикаем
                message = f'🛡️ @{target_display}(Пользователь) получил максимальное количество выговоров и был исключен из чата.'
                self.send_message(peer_id, message)

                # Кикаем пользователя
                kick_result = self.kick_user(chat_id, target_id, f"Максимальное количество предупреждений (3/3). Последняя причина: {reason}")

                if kick_result:
                    self.log(f"Пользователь {target_display} исключен из чата {chat_id} за 3 предупреждения")
                else:
                    self.send_message(peer_id, '❌ Не удалось исключить пользователя. Возможно, он уже покинул беседу или имеет права администратора.')
            else:
                message = f'🚫 [id{target_id}|{target_display}] получил {warning_count}/3 предупреждений в чате.\n❓ Причина: {reason}'
                self.send_message(peer_id, message)
                self.log(f"Предупреждение выдано пользователю {target_display}. Причина: {reason}")

        except Exception as e:
            self.log(f"Ошибка выдачи предупреждения: {e}")
            self.send_message(peer_id, '❌ Ошибка при выдаче предупреждения.')

    def command_kick(self, peer_id, sender_id, target_id, reason, chat_id):
        if not target_id:
            error_message = """⛔️ Отказано! Вы не указали пользователя для исключения пользователя из чата.

☕️ Примеры использования:
/kick @durov 1.1
/kick - ответом на сообщение"""
            self.send_message(peer_id, error_message)
            return

        # Если причина не указана, устанавливаем стандартную
        if not reason or not reason.strip():
            reason = "Не указана"

        moderation_check = self.can_moderate_user(sender_id, target_id, chat_id)
        if not moderation_check['can_moderate']:
            self.send_message(peer_id, moderation_check['reason'])
            return

        try:
            kick_result = self.kick_user(chat_id, target_id, reason)

            target_display = self.get_display_name(target_id, chat_id)
            sender_display = self.get_display_name(sender_id, chat_id)

            if kick_result:
                kick_message = f'''☕️ [id{target_id}|{target_display}] исключен из чата!

☕️ Причина: {reason}
☕️ Администратор: [id{sender_id}|{sender_display}]'''

                # Создаем inline-кнопку для бана
                keyboard = {
                    "inline": True,
                    "buttons": [
                        [{
                            "action": {
                                "type": "callback",
                                "label": "🔴 Забанить навсегда",
                                "payload": json.dumps({
                                    "action": "ban_forever",
                                    "user_id": target_id,
                                    "chat_id": chat_id,
                                    "reason": reason
                                })
                            },
                            "color": "negative"
                        }]
                    ]
                }

                self.send_message(peer_id, kick_message, json.dumps(keyboard))
                self.log(f"Пользователь {target_display} исключен из чата {chat_id}. Причина: {reason}")
            else:
                self.send_message(peer_id, '❌ Не удалось исключить пользователя. Возможно, он уже покинул беседу или имеет права администратора.')
        except Exception as e:
            self.log(f"Ошибка кика пользователя: {e}")
            self.send_message(peer_id, '❌ Не удалось исключить пользователя. Возможно, он уже покинул беседу или имеет права администратора.')

    def command_ban(self, peer_id, sender_id, target_id, reason, chat_id, days=None):
        if not target_id:
            error_message = """⛔️ Отказано! Вы не указали пользователя для блокировки пользователя из чата.

☕️ Примеры использования:
/ban @durov 30 причина
/ban @durov 30
/ban - ответом на сообщение"""
            self.send_message(peer_id, error_message)
            return

        # Проверяем права - модератор и выше
        if not self.has_permission(sender_id, None, 40, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /ban доступен от Модератора (40) и выше.')
            return

        moderation_check = self.can_moderate_user(sender_id, target_id, chat_id)
        if not moderation_check['can_moderate']:
            self.send_message(peer_id, moderation_check['reason'])
            return

        try:
            # Если причина не указана, устанавливаем "Не указана"
            if not reason or not reason.strip():
                reason = "Не указана"

            target_display = self.get_display_name(target_id, chat_id)

            # Проверяем, не забанен ли уже пользователь
            existing_ban = self.db.get_user_ban_in_chat(target_id, chat_id)

            if existing_ban:
                # Если уже забанен и указаны дни - продлеваем бан
                if days and days > 0:
                    # Получаем текущую дату окончания бана или текущее время
                    cursor = self.db.conn.cursor()
                    cursor.execute(
                        'SELECT banned_until FROM chat_bans WHERE user_id = ? AND chat_id = ? AND is_active = 1',
                        (target_id, chat_id)
                    )
                    result = cursor.fetchone()

                    if result and result['banned_until']:
                        try:
                            current_ban_until = datetime.fromisoformat(result['banned_until'])
                        except:
                            current_ban_until = datetime.now()
                    else:
                        current_ban_until = datetime.now()

                    # Продлеваем на указанное количество дней
                    new_ban_until = current_ban_until + timedelta(days=days)

                    # Обновляем бан с новой датой
                    cursor.execute('''
                        UPDATE chat_bans 
                        SET banned_until = ?, reason = ?, banned_by = ?
                        WHERE user_id = ? AND chat_id = ? AND is_active = 1
                    ''', (new_ban_until.isoformat(), reason, sender_id, target_id, chat_id))
                    self.db.conn.commit()

                    months = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 
                             'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
                    ban_date_str = f"{new_ban_until.day} {months[new_ban_until.month - 1]} {new_ban_until.year} г. в {new_ban_until.strftime('%H:%M')} GMT+3"

                    ban_message = f'☕️ Блокировка [id{target_id}|{target_display}] продлена до {ban_date_str}.\n✄ Причина: {reason}'
                    self.send_message(peer_id, ban_message)
                    self.log(f"Блокировка пользователя {target_display} продлена на {days} дней до {new_ban_until}")
                    return
                else:
                    self.send_message(peer_id, f'⛔️ [id{target_id}|{target_display}] уже заблокирован в этом чате. Для продления укажите количество дней.')
                    return

            # Добавляем новый бан
            cursor = self.db.conn.cursor()

            # Вычисляем дату окончания бана если указаны дни
            ban_until = None
            if days and days > 0:
                ban_until = (datetime.now() + timedelta(days=days)).isoformat()

            # Деактивируем старые баны
            cursor.execute(
                'UPDATE chat_bans SET is_active = 0 WHERE user_id = ? AND chat_id = ?',
                (target_id, chat_id)
            )

            # Добавляем новый бан с датой окончания
            cursor.execute(
                'INSERT INTO chat_bans (user_id, chat_id, reason, banned_by, banned_until) VALUES (?, ?, ?, ?, ?)',
                (target_id, chat_id, reason, sender_id, ban_until)
            )
            self.db.conn.commit()

            # Пытаемся кикнуть пользователя (если он в беседе)
            kick_result = self.kick_user(chat_id, target_id, reason)

            # Получаем никнейм администратора
            sender_display = self.get_display_name(sender_id, chat_id)

            # Форматируем сообщение в зависимости от наличия дней
            if days and days > 0:
                ban_until_dt = datetime.now() + timedelta(days=days)
                months = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 
                         'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
                ban_date_str = f"{ban_until_dt.day} {months[ban_until_dt.month - 1]} {ban_until_dt.year} г. в {ban_until_dt.strftime('%H:%M')}"

                ban_message = f'''🍸 [id{target_id}|{target_display}] получил блокировку на {days} дн.

☕️Истекает: {ban_date_str}
☕️Причина: {reason}
☕️Администратор: [id{sender_id}|{sender_display}]'''
            else:
                ban_message = f'''☕️ [id{target_id}|{target_display}] получил блокировку навсегда

Истекает: Никогда
Причина: {reason}
Администратор: [id{sender_id}|{sender_display}]'''

            # Создаем inline-кнопку для снятия блокировки
            keyboard = {
                "inline": True,
                "buttons": [
                    [{
                        "action": {
                            "type": "callback",
                            "label": "🔴 Снять блокировку",
                            "payload": json.dumps({
                                "action": "unban_user",
                                "user_id": target_id,
                                "chat_id": chat_id
                            })
                        },
                        "color": "negative"
                    }]
                ]
            }

            self.send_message(peer_id, ban_message, json.dumps(keyboard))
            self.log(f"Пользователь {target_display} заблокирован в чате {chat_id} на {days if days else 'бессрочно'} дней. Причина: {reason}")

        except Exception as e:
            self.log(f"Ошибка блокировки пользователя: {e}")
            self.send_message(peer_id, '❌ Ошибка при блокировке пользователя.')

    def command_mute(self, peer_id, sender_id, target_id, duration, reason, chat_id):
        if not target_id:
            error_message = """☕️ Отказано! Вы не указали пользователя для блокировки чата.

☕️ Примеры:
/mute @durov 60 причина
/mute @durov 30
/mute - ответом на сообщение"""
            self.send_message(peer_id, error_message)
            return

        # Если причина не указана, устанавливаем стандартную
        if not reason or not reason.strip():
            reason = "Не указана"

        moderation_check = self.can_moderate_user(sender_id, target_id, chat_id)
        if not moderation_check['can_moderate']:
            self.send_message(peer_id, moderation_check['reason'])
            return

        try:
            mute_until = datetime.now() + timedelta(minutes=duration)
            self.db.add_mute(target_id, chat_id, reason, sender_id, mute_until)

            target_display = self.get_display_name(target_id, chat_id)
            sender_display = self.get_display_name(sender_id, chat_id)

            # Форматируем дату окончания мута
            mute_date_str = f"{mute_until.day:02d}.{mute_until.month:02d}.{mute_until.year} в {mute_until.strftime('%H:%M')}"

            # Формируем текст длительности
            if duration == 1:
                duration_text = "1 минуту"
            elif duration < 5:
                duration_text = f"{duration} минуты"
            elif duration < 60:
                duration_text = f"{duration} минут"
            elif duration == 60:
                duration_text = "1 час"
            elif duration < 120:
                hours = duration // 60
                duration_text = f"{hours} час"
            elif duration < 300:
                hours = duration // 60
                duration_text = f"{hours} часа"
            else:
                hours = duration // 60
                duration_text = f"{hours} часов"

            mute_message = f'''☕️ [id{target_id}|{target_display}] получил блокировку отправки сообщений на {duration_text}

☕️ Истекает: {mute_date_str}
☕️ Причина: {reason}
☕️ Администратор: [id{sender_id}|{sender_display}]'''

            self.send_message(peer_id, mute_message)
            self.log(f"Пользователь {target_display} заглушен до {mute_date_str}. Причина: {reason}")

        except Exception as e:
            self.log(f"Ошибка заглушения пользователя: {e}")
            self.send_message(peer_id, '❌ Ошибка при заглушении пользователя.')

    def command_unmute(self, peer_id, sender_id, target_id, chat_id):
        if not target_id:
            error_message = """☕️ Отказано! Вы не указали пользователя для снятие блокировки чата.

☕️ Примеры:
/unmute @durov
/unmute ответом на сообщение"""
            self.send_message(peer_id, error_message)
            return

        try:
            active_mute = self.db.get_active_mute_in_chat(target_id, chat_id)

            if active_mute:
                self.db.remove_mute(target_id, chat_id)

                target_display = self.get_display_name(target_id, chat_id)

                self.send_message(peer_id, f'🔊 [id{target_id}|{target_display}] больше не заглушен.')
                self.log(f"Пользователь {target_display} разглушен в чате {chat_id}")
            else:
                target_display = self.get_display_name(target_id, chat_id)
                self.send_message(peer_id, f'❌ У [id{target_id}|{target_display}] нет активного мута в этом чате.')
        except Exception as e:
            self.log(f"Ошибка снятия мута: {e}")
            self.send_message(peer_id, '❌ Ошибка при снятии заглушения.')

    def command_unban(self, peer_id, sender_id, target_id, chat_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        # Проверяем права - модератор и выше
        if not self.has_permission(sender_id, None, 40, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /unban дотсупен с Модератора (40) и выше.')
            return

        try:

            user_ban = self.db.get_user_ban_in_chat(target_id, chat_id)

            if user_ban:
                self.db.remove_chat_ban(target_id, chat_id)

                target_info = self.get_user_info(target_id)
                target_name = target_info['screen_name'] if target_info else str(target_id)

                self.send_message(peer_id, f'♻️ @{target_name}(Пользователь) был разблокирован.')
                self.log(f"Пользователь {target_name} разблокирован в чате {chat_id}")
            else:
                self.send_message(peer_id, '❌ Пользователь не заблокирован в этом чате.')
        except Exception as e:
            self.log(f"Ошибка разбана пользователя: {e}")
            self.send_message(peer_id, '❌ Ошибка при разблокировке пользователя.')

    def start_mute_checker(self):
        """Запустить периодическую проверку истекших мутов"""
        def check_expired_mutes():
            try:
                expired_mutes = self.db.get_expired_mutes()
                for mute in expired_mutes:
                    # Снимаем мут
                    self.db.remove_mute(mute['id'])

                    # Отправляем уведомление в чат
                    chat_id = mute['chat_id']
                    user_id = mute['user_id']
                    peer_id = 2000000000 + chat_id

                    # Получаем информацию о пользователе
                    user_info = self.get_user_info(user_id)
                    user_name = user_info['screen_name'] if user_info else str(user_id)

                    message = f"⚠️ У @{user_name}(Пользователя) закончилась блокировка чата."
                    self.send_message(peer_id, message)
                    self.log(f"Автоматически снят мут с пользователя {user_id} в чате {chat_id}")

            except Exception as e:
                self.log(f"Ошибка при проверке истекших мутов: {e}")

            # Запускаем следующую проверку через 60 секунд
            self.mute_check_timer = threading.Timer(60.0, check_expired_mutes)
            self.mute_check_timer.daemon = True
            self.mute_check_timer.start()

        # Первая проверка через 10 секунд после запуска
        self.mute_check_timer = threading.Timer(10.0, check_expired_mutes)
        self.mute_check_timer.daemon = True
        self.mute_check_timer.start()
        self.log("Система автоматической проверки мутов запущена")

    def command_balance(self, peer_id, user_id):
        try:
            user_balance = self.db.get_user_balance(user_id)
            balance_text = f"💰 Ваш баланс: {user_balance['balance']:,} $ \n🎁 Бонусы: {user_balance['bonus_points']:,} $"
            self.send_message(peer_id, balance_text)
        except Exception as e:
            self.log(f"Ошибка получения баланса: {e}")
            self.send_message(peer_id, '❌ Ошибка получения баланса.')

    def command_report(self, peer_id, user_id, text):
        if not text:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать текст репорта.')
            return

        try:
            user_info = self.get_user_info(user_id)
            username = user_info['screen_name'] if user_info else str(user_id)

            ticket_id = self.db.create_support_ticket(user_id, username, peer_id, text)

            self.send_message(peer_id, f'📋 Ваш репорт принят в обработку!\n🎫 ID тикета: {ticket_id}\n\n⏰ Ожидайте ответа от службы поддержки.')
            self.log(f"Тикет #{ticket_id} создан пользователем {username}: {text}")

            # Отправляем уведомление в чат поддержки
            support_chat_id = self.db.get_support_chat()
            if support_chat_id:
                support_message = f"""☏ Новый репорт #{ticket_id}

✄ От: [id{user_id}|{username}]
✎ Вопрос: {text}

✵ !answer {ticket_id} [ответ]"""

                self.send_message(support_chat_id, support_message)

        except Exception as e:
            self.log(f"Ошибка создания тикета: {e}")
            self.send_message(peer_id, '❌ Ошибка при создании тикета поддержки.')

    def command_newrole(self, peer_id, sender_id, target_id, role_level, role_name, chat_id):
        if not role_level or not role_name:
            self.send_message(peer_id, '⛔️ Отказано! Использование: /newrole [приоретет] [название]')
            return

        # Проверяем, что команда используется в групповом чате
        if not chat_id:
            self.send_message(peer_id, '❌ Кастомные роли можно создавать только в групповых чатах.')
            return

        try:
            role_level = int(role_level)
        except ValueError:
            self.send_message(peer_id, '❌ Уровень роли должен быть числом.')
            return

        if role_level < 0 or role_level > 100:
            self.send_message(peer_id, '⛔️ Отказано! Приоретет роли должен быть от 0 до 100.')
            return

        # Проверяем, не существует ли уже роль с таким уровнем в стандартных ролях
        is_system_role = role_level in CONFIG['roles']

        sender_role = self.get_user_role(sender_id, chat_id)

        # Для изменения роли 100 требуется уровень 100
        if role_level == 100 and sender_role['level'] < 100:
            self.send_message(peer_id, '⛔️ Отказано! Только создатели чата могут изменять роль с приорететом 100.')
            return

        if sender_role['level'] < 40:
            self.send_message(peer_id, '❌ Только администраторы и выше могут создавать роли.')
            return

        # Для не-владельцев: нельзя создавать/изменять роли своего уровня или выше
        if sender_role['level'] < 100 and role_level >= sender_role['level']:
            self.send_message(peer_id, f'⛔️ Отказано! Вы не можете создать роль с уровнем {role_level} или выше вашего ({sender_role["level"]}).')
            return

        try:
            cursor = self.db.conn.cursor()

            # Проверяем, существует ли уже активная кастомная роль с таким уровнем в этом чате
            cursor.execute(
                'SELECT id, role_name FROM custom_role_definitions WHERE chat_id = ? AND role_level = ? AND is_active = 1',
                (chat_id, role_level)
            )
            existing_role = cursor.fetchone()

            if existing_role:
                # Кастомная роль уже существует - обновляем название
                old_name = existing_role['role_name']
                role_id = existing_role['id']

                # Обновляем название роли в определениях
                cursor.execute('''
                    UPDATE custom_role_definitions 
                    SET role_name = ?, created_by = ?, created_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (role_name, sender_id, role_id))

                # Обновляем название роли у всех пользователей в этом чате
                cursor.execute('''
                    UPDATE chat_roles 
                    SET role_name = ?
                    WHERE chat_id = ? AND role_level = ? AND is_active = 1
                ''', (role_name, chat_id, role_level))

                self.log(f"Обновлено название роли с '{old_name}' на '{role_name}' (уровень {role_level}) в чате {chat_id}")
                self.send_message(peer_id, f'✅ Роль "{old_name}" с приоритетом [{role_level}] успешно обновлена на "{role_name}"')

            elif is_system_role:
                # Системная роль - создаем кастомное определение для изменения названия
                old_name = CONFIG['roles'][role_level]

                # Создаем запись для системной роли с измененным названием
                cursor.execute('''
                    INSERT INTO custom_role_definitions 
                    (chat_id, role_level, role_name, created_by, created_at, is_active)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 1)
                ''', (chat_id, role_level, role_name, sender_id))

                # Обновляем название роли у всех пользователей в этом чате
                cursor.execute('''
                    UPDATE chat_roles 
                    SET role_name = ?
                    WHERE chat_id = ? AND role_level = ? AND is_active = 1
                ''', (role_name, chat_id, role_level))

                self.log(f"Обновлено название системной роли с '{old_name}' на '{role_name}' (уровень {role_level}) в чате {chat_id}")
                self.send_message(peer_id, f'✅ Роль "{old_name}" с приоритетом [{role_level}] успешно обновлена на "{role_name}"!')

            else:
                # Проверяем, есть ли удаленная роль с таким уровнем
                cursor.execute(
                    'SELECT id FROM custom_role_definitions WHERE chat_id = ? AND role_level = ? AND is_active = 0',
                    (chat_id, role_level)
                )
                deleted_role = cursor.fetchone()

                if deleted_role:
                    # Реактивируем удаленную роль с новым названием
                    cursor.execute('''
                        UPDATE custom_role_definitions 
                        SET role_name = ?, created_by = ?, created_at = CURRENT_TIMESTAMP, is_active = 1
                        WHERE id = ?
                    ''', (role_name, sender_id, deleted_role['id']))
                    self.log(f"Реактивирована кастомная роль '{role_name}' (уровень {role_level}) в чате {chat_id}")
                    self.send_message(peer_id, f'✅ Новая роль "{role_name}" с приоритетом [{role_level}] успешно создана!')
                else:
                    # Создаем новое определение кастомной роли
                    cursor.execute('''
                        INSERT INTO custom_role_definitions 
                        (chat_id, role_level, role_name, created_by, created_at, is_active)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 1)
                    ''', (chat_id, role_level, role_name, sender_id))
                    self.log(f"Создана кастомная роль '{role_name}' (уровень {role_level}) в чате {chat_id}")
                    self.send_message(peer_id, f'✅ Роль "{role_name}" с приоритетом [{role_level}] успешно создана!')

            self.db.conn.commit()

        except Exception as e:
            self.log(f"Ошибка создания кастомной роли: {e}")
            self.send_message(peer_id, '❌ Ошибка при создании кастомной роли.')

    def command_stats(self, peer_id, user_id, target_id=None, chat_id=None):
        try:
            if target_id:
                user_info = self.get_user_info(target_id)
            else:
                user_info = self.get_user_info(user_id)
                target_id = user_id

            username = user_info['screen_name'] if user_info else str(target_id)

            # Проверка на сотрудника в боте (системный админ)
            system_admin = self.db.get_system_admin(target_id)
            is_staff = "Да" if system_admin else "Нет"

            # Получаем роль в беседе
            role_text = "Пользователь"
            nickname_text = "Нет"
            message_count = 0
            invite_date = "Неизвестно"

            if chat_id:
                # Роль в чате
                chat_role = self.db.get_chat_role(target_id, chat_id)
                if chat_role:
                    chat_role_dict = dict(chat_role)
                    if chat_role_dict.get('role_level', 0) > 0:
                        role_text = chat_role_dict['role_name']

                # Никнейм в беседе
                nickname = self.db.get_user_nickname(target_id, chat_id)
                if nickname:
                    nickname_text = nickname

                # Количество сообщений
                user = self.db.get_user(target_id)
                if user:
                    user_dict = dict(user)
                    message_count = user_dict.get('message_count', 0)

                    # Дата приглашения
                    if user_dict.get('created_at'):
                        try:
                            created_at = datetime.strptime(user_dict['created_at'], '%Y-%m-%d %H:%M:%S')
                            months = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 
                                     'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
                            invite_date = f"{created_at.day} {months[created_at.month - 1]} {created_at.year} года в {created_at.strftime('%H:%M')}"
                        except:
                            invite_date = user_dict.get('created_at', 'Неизвестно')

            # Предупреждения
            warnings = len(self.db.get_user_warnings(target_id)) if chat_id else 0

            stats_text = f"""💎 Информация о [id{target_id}|пользователе]:

💼 Сотрудник в боте: {is_staff}
🌀 Статус: {role_text}
🗒️ Никнейм в чате: {nickname_text}
💬 Активность: {message_count}
⚠️ Предупреждения: {warnings}/3
📅 Дата приглашения: {invite_date}"""

            self.send_message(peer_id, stats_text)
        except Exception as e:
            self.log(f"Ошибка получения статистики: {e}")
            self.send_message(peer_id, '❌ Ошибка получения статистики.')

    def command_online(self, peer_id, chat_id):
        try:
            response = self.api_request('messages.getConversationMembers', {
                'peer_id': peer_id
            })

            if not response:
                self.send_message(peer_id, '❌ Ошибка получения участников чата.')
                return

            online_users = []
            for profile in response.get('profiles', []):
                if profile.get('online'):
                    online_users.append(f"@{profile.get('screen_name', profile['id'])}")

            online_text = f"""🟢 Пользователи онлайн: {len(online_users)}

{chr(10).join(online_users) if online_users else '⛔️ К сожалению, сейчас нет пользователей в сети.'}"""
            self.send_message(peer_id, online_text)
        except Exception as e:
            self.log(f"Ошибка получения онлайн: {e}")
            self.send_message(peer_id, '❌ Ошибка получения списка онлайн.')

    def command_staff(self, peer_id, chat_id):
        try:
            staff_text = "☕️ Список Администрации:\n\n"

            if chat_id:
                chat_roles = self.db.get_all_chat_roles(chat_id)
                role_groups = {}

                for role in chat_roles:
                    if role['role_level'] >= 10 and role['user_id'] > 0:  # Показываем только пользователей, не группы
                        role_name = role['role_name']
                        if role_name not in role_groups:
                            role_groups[role_name] = []

                        user_info = self.get_user_info(role['user_id'])
                        username = user_info['screen_name'] if user_info else str(role['user_id'])

                        # Получаем полное имя пользователя
                        full_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip() if user_info else username

                        role_groups[role_name].append({
                            'user_id': role['user_id'],
                            'username': username,
                            'full_name': full_name,
                            'level': role['role_level']
                        })

                # Сортируем роли по уровню
                sorted_roles = sorted(role_groups.items(), key=lambda x: max(r['level'] for r in x[1]), reverse=True)

                for role_name, members in sorted_roles:
                    staff_text += f"{role_name}:\n"
                    for member in members:
                        # Используем формат [id|Имя] если есть полное имя, иначе @username
                        if member['full_name'] and member['full_name'] != member['username']:
                            staff_text += f"— [id{member['user_id']}|{member['full_name']}]\n"
                        else:
                            staff_text += f"— @{member['username']}\n"
                    staff_text += "\n"

                if not role_groups:
                    staff_text += "❌ Нет администрации в этом чате\n"
            else:
                staff_text += "❌ Команда доступна только в групповых чатах\n"

            self.send_message(peer_id, staff_text)
        except Exception as e:
            self.log(f"Ошибка получения администрации: {e}")
            self.send_message(peer_id, '❌ Ошибка получения списка администрации.')

    def command_unwarn(self, peer_id, sender_id, target_id, chat_id):
        if not target_id:
            help_text = """☕️ Отказано! Что выдать наказание укажите пользователя.

☕️ Примеры:
/unwarn @durov
/unwarn [ответ на сообщение]"""
            self.send_message(peer_id, help_text)
            return

        moderation_check = self.can_moderate_user(sender_id, target_id, chat_id)
        if not moderation_check['can_moderate']:
            self.send_message(peer_id, moderation_check['reason'])
            return

        try:
            warnings = self.db.get_user_warnings(target_id)
            if not warnings:
                self.send_message(peer_id, '❌ У пользователя нет предупреждений.')
                return

            self.db.remove_warning(target_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.send_message(peer_id, f'✅ Предупреждение снято с @{target_name}(пользоваетля).')
        except Exception as e:
            self.log(f"Ошибка снятия предупреждения: {e}")
            self.send_message(peer_id, '❌ Ошибка при снятии предупреждения.')

    def command_getwarn(self, peer_id, target_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо укзаать пользователя.')
            return

        try:
            warnings = self.db.get_user_warnings(target_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            warn_text = f"""⚠️ @{target_name}(Пользователь):
🔢 Всего: {len(warnings)}

"""
            for i, warning in enumerate(warnings[:5], 1):
                warn_text += f"{i}. {warning['reason']}\n"
                warn_text += f"   🚫 {warning['created_at'][:10]}\n\n"
            self.send_message(peer_id, warn_text)
        except Exception as e:
            self.log(f"Ошибка получения предупреждений: {e}")
            self.send_message(peer_id, '❌ Ошибка получения предупреждений.')

    def command_getreport(self, peer_id, user_id):
        try:
            tickets = self.db.get_user_tickets(user_id)

            report_text = f"""📋 Ваши тикеты:

🎫 Всего тикетов: {len(tickets)}

"""
            for ticket in tickets[:5]:
                status_emoji = "🟢" if ticket['status'] == 'open' else "🔴"
                report_text += f"{status_emoji} #{ticket['id']} - {ticket['status']}\n"
                report_text += f"   📝 {ticket['message'][:50]}...\n"
                report_text += f"   📅 {ticket['created_at'][:10]}\n\n"
            self.send_message(peer_id, report_text)
        except Exception as e:
            self.log(f"Ошибка получения тикетов: {e}")
            self.send_message(peer_id, '❌ Ошибка получения ваших тикетов.')

    def command_helper(self, peer_id, sender_id, target_id, chat_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        # Проверка, что пользователь не пытается изменить свою собственную роль
        if target_id == sender_id:
            self.send_message(peer_id, '⛔️ Отказано! Вы не можете выдавать роль, приоритет которой выше или равен вашему.')
            return

        if not self.has_permission(sender_id, None, 40, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /helper доступен с Модератора (40) и выше.')
            return

        try:
            role_name = self.get_role_name_for_level(20, chat_id)
            self.db.set_chat_role(target_id, chat_id, 20, role_name, sender_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.send_message(peer_id, f'✅ @{target_name}(Пользователь) получил новую роль "{role_name}" (20).')
        except Exception as e:
            self.log(f"Ошибка назначения роли помощника: {e}")
            self.send_message(peer_id, '❌ Ошибка при назначении роли.')

    def command_gm(self, peer_id, sender_id, target_id, chat_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
            return

        if not self.has_permission(sender_id, None, 'admin', chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /gm доступен с Администартора (60) и выше.')
            return

        try:
            self.db.set_immunity(target_id, sender_id, chat_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.send_message(peer_id, f'🛡️ @{target_name}(Пользователю) выдан иммунитет от наказаний в этом чате.')
        except Exception as e:
            self.log(f"Ошибка выдачи иммунитета: {e}")
            self.send_message(peer_id, '❌ Ошибка при выдаче иммунитета.')

    def command_gms(self, peer_id, chat_id):
        try:
            if not chat_id:
                self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
                return

            immunities = self.db.get_all_immunities(chat_id)

            gms_text = f"""🛡️ Список пользователей с иммунитетом в этом чате:

"""
            if immunities:
                for immunity in immunities:
                    user_info = self.get_user_info(immunity['user_id'])
                    username = user_info['screen_name'] if user_info else str(immunity['user_id'])
                    gms_text += f"🔸 @{username}\n"
            else:
                gms_text += "❌ Нет пользователей с иммунитетом в этом чате\n"
            self.send_message(peer_id, gms_text)
        except Exception as e:
            self.log(f"Ошибка получения списка иммунитетов: {e}")
            self.send_message(peer_id, '❌ Ошибка получения списка иммунитетов.')

    def command_grm(self, peer_id, sender_id, target_id, chat_id):
        """Снять иммунитет у пользователя"""
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
            return

        if not self.has_permission(sender_id, None, 'admin', chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /grm доступен с Администратора (60) и выше')
            return

        try:
            # Проверяем, есть ли у пользователя иммунитет
            immunity = self.db.get_immunity(target_id, chat_id)
            if not immunity:
                self.send_message(peer_id, '❌ У этого пользователя нет иммунитета в этом чате.')
                return

            # Снимаем иммунитет
            self.db.remove_immunity(target_id, chat_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.send_message(peer_id, f'🛡️ У @{target_name} снят иммунитет от наказаний в этом чате.')
        except Exception as e:
            self.log(f"Ошибка снятия иммунитета: {e}")
            self.send_message(peer_id, '❌ Ошибка при снятии иммунитета.')

    def command_banlist(self, peer_id, chat_id):
        try:
            bans = self.db.get_all_active_chat_bans(chat_id)

            banlist_text = "✄ Список заблокированных пользователей:\n\n"

            if bans:
                for i, ban in enumerate(bans, 1):
                    user_info = self.get_user_info(ban['user_id'])
                    if user_info:
                        full_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip()
                        if not full_name:
                            full_name = user_info.get('screen_name', str(ban['user_id']))
                    else:
                        full_name = str(ban['user_id'])

                    banlist_text += f"{i}. [id{ban['user_id']}|{full_name}]\n"
            else:
                banlist_text += "✅ Нет заблокированных пользователей.\n"

            self.send_message(peer_id, banlist_text)
        except Exception as e:
            self.log(f"Ошибка получения списка банов: {e}")
            self.send_message(peer_id, '❌ Ошибка получения списка заблокированных.')

    def command_top(self, peer_id):
        try:
            top_users = self.db.get_top_users(10)

            top_text = """🏆 Топ пользователей по количеству сообщений:

"""
            for i, user in enumerate(top_users, 1):
                user_info = self.get_user_info(user['user_id'])
                username = user_info['screen_name'] if user_info else str(user['user_id'])
                top_text += f"{i}. @{username} - {user['message_count']:,} сообщений\n"

            if not top_users:
                top_text += "❌ Нет данных о пользователях\n"
            self.send_message(peer_id, top_text)
        except Exception as e:
            self.log(f"Ошибка получения топа: {e}")
            self.send_message(peer_id, '❌ Ошибка получения топа пользователей.')

    def command_mtop(self, peer_id):
        try:
            top_users = self.db.get_top_users_by_balance(10)

            top_text = "💰 Топ по балансу\n\n"

            for i, user in enumerate(top_users, 1):
                user_info = self.get_user_info(user['user_id'])
                username = user_info['screen_name'] if user_info else str(user['user_id'])
                balance = user['balance']

                # Форматируем баланс
                if balance >= 1000000:
                    balance_display = f"{balance/1000000:.1f}кк$"
                elif balance >= 1000:
                    balance_display = f"{balance/1000:.1f}к$"
                else:
                    balance_display = f"{balance}$"

                top_text += f"[{i}]. [id{user['user_id']}|{username}] - {balance_display}\n"

            if not top_users:
                top_text += "❌ Нет пользователей с балансом\n"

            self.send_message(peer_id, top_text)
        except Exception as e:
            self.log(f"Ошибка получения топа по балансу: {e}")
            self.send_message(peer_id, '❌ Ошибка получения топа по балансу.')

    def command_answer(self, peer_id, sender_id, ticket_id, answer):
        if not ticket_id or not answer:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать ID тикета и ответ.')
            return

        try:
            ticket_id = int(ticket_id)
        except ValueError:
            self.send_message(peer_id, '❌ ID тикета должен быть числом.')
            return

        system_admin = self.db.get_system_admin(sender_id)
        if not system_admin or system_admin['access_level'] < 1:
            self.send_message(peer_id, '❌ У вас нет прав для ответа на тикеты.')
            return

        try:
            # Получаем тикет
            ticket = self.db.get_ticket_by_id(ticket_id)
            if not ticket:
                self.send_message(peer_id, f'❌ Тикет #{ticket_id} не найден.')
                return

            # Проверяем, не был ли уже дан ответ
            if ticket['status'] == 'answered':
                self.send_message(peer_id, f'❌ На тикет #{ticket_id} уже был дан ответ.')
                return

            # Отмечаем тикет как отвеченный
            self.db.answer_ticket(ticket_id, answer, sender_id)

            # Получаем информацию о том, кто ответил
            responder_info = self.get_user_info(sender_id)
            responder_name = responder_info['screen_name'] if responder_info else str(sender_id)

            # Отправляем ответ в беседу пользователя
            user_chat_id = ticket['chat_id']
            answer_message = f"""📬 Ответ от поддержки:

☕️ | {answer}

👤 Ответил: @{responder_name}"""

            self.send_message(user_chat_id, answer_message)

            # Уведомляем в чате поддержки об успешном ответе
            self.send_message(peer_id, f'✅ Ответ на тикет #{ticket_id} отправлен пользователю.')
            self.log(f"Ответ на тикет #{ticket_id} отправлен {responder_name}")

        except Exception as e:
            self.log(f"Ошибка ответа на тикет: {e}")
            self.send_message(peer_id, '❌ Ошибка при ответе на тикет.')

    def command_tickets(self, peer_id, sender_id):
        """Показать все тикеты поддержки"""
        system_admin = self.db.get_system_admin(sender_id)
        if not system_admin or system_admin['access_level'] < 1:
            self.send_message(peer_id, '❌ У вас нет прав для просмотра тикетов.')
            return

        try:
            cursor = self.db.conn.cursor()
            cursor.execute('''
                SELECT * FROM support_tickets 
                ORDER BY 
                    CASE status 
                        WHEN 'open' THEN 1 
                        ELSE 2 
                    END,
                    created_at DESC 
                LIMIT 20
            ''')
            tickets = cursor.fetchall()

            if not tickets:
                self.send_message(peer_id, '📋 Нет тикетов в системе.')
                return

            tickets_text = "📋 СПИСОК ТИКЕТОВ ПОДДЕРЖКИ\n\n"

            open_tickets = [t for t in tickets if t['status'] == 'open']
            closed_tickets = [t for t in tickets if t['status'] != 'open']

            if open_tickets:
                tickets_text += "🟢 ОТКРЫТЫЕ ТИКЕТЫ:\n"
                for ticket in open_tickets:
                    tickets_text += f"#{ticket['id']} - @{ticket['username']}\n"
                    tickets_text += f"   📝 {ticket['message'][:50]}...\n"
                    tickets_text += f"   📅 {ticket['created_at'][:16]}\n\n"

            if closed_tickets:
                tickets_text += "\n🔴 ЗАКРЫТЫЕ ТИКЕТЫ:\n"
                for ticket in closed_tickets[:5]:
                    tickets_text += f"#{ticket['id']} - @{ticket['username']}\n"
                    tickets_text += f"   📝 {ticket['message'][:50]}...\n"
                    tickets_text += f"   📅 {ticket['created_at'][:16]}\n\n"

            tickets_text += f"\n📊 Всего: {len(tickets)} тикетов"

            self.send_message(peer_id, tickets_text)

        except Exception as e:
            self.log(f"Ошибка получения списка тикетов: {e}")
            self.send_message(peer_id, '❌ Ошибка получения списка тикетов.')

    def command_settoken(self, peer_id):
        self.send_message(peer_id, '🔑 Система токенов пока не реализована. Обратитесь к администратору.')

    def command_silence(self, peer_id, sender_id, chat_id):
        if not self.has_permission(sender_id, None, 'helper', chat_id):
            self.send_message(peer_id, '❌ У вас нет прав для включения режима тишины.')
            return

        self.send_message(peer_id, '🔇 Режим тишины активирован. Все сообщения будут удаляться автоматически.')

    def command_getbynick(self, peer_id, nickname):
        if not nickname:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать никнейм.')
            return

        try:
            user = self.db.get_user_by_nickname(nickname)
            if user:
                user_info = self.get_user_info(user['user_id'])
                username = user_info['screen_name'] if user_info else str(user['user_id'])
                self.send_message(peer_id, f'👤 Пользователь с никнеймом "{nickname}": @{username}')
            else:
                self.send_message(peer_id, f'❌ Пользователь с никнеймом "{nickname}" не найден.')
        except Exception as e:
            self.log(f"Ошибка поиска по никнейму: {e}")
            self.send_message(peer_id, '❌ Ошибка поиска по никнейму.')

    def command_warnhistory(self, peer_id, target_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        try:
            history = self.db.get_warn_history(target_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            history_text = f"""Истории предупрждений:

👤 Пользователь: @{target_name}
📊 Всего записей: {len(history)}

"""
            for i, warn in enumerate(history[:10], 1):
                warned_by_name = warn['warned_by_name'] or str(warn['warned_by'])
                history_text += f"{i}. {warn['reason']}\n"
                history_text += f"   👮 Выдал: {warned_by_name}\n"
                history_text += f"   📅 {warn['created_at'][:16]}\n\n"
            self.send_message(peer_id, history_text)
        except Exception as e:
            self.log(f"Ошибка получения истории: {e}")
            self.send_message(peer_id, '❌ Ошибка получения истории предупреждений.')

    def command_warnlist(self, peer_id, chat_id):
        try:
            users_with_warnings = self.db.get_users_with_warnings(chat_id)

            if not users_with_warnings:
                self.send_message(peer_id, '✅ Нет пользователей с предупреждениями')
                return

            warnlist_text = "⚠️ Список пользователей с предупреждениями:\n\n"

            for user_data in users_with_warnings:
                user_id = user_data['user_id']
                warning_count = user_data['warning_count']
                warnlist_text += f"[id{user_id}|@id{user_id}] — {warning_count}/3\n"

            warnlist_text += f"\nВсего пользователей с предупреждениями: {len(users_with_warnings)}"

            self.send_message(peer_id, warnlist_text)
        except Exception as e:
            self.log(f"Ошибка получения списка предупреждений: {e}")
            self.send_message(peer_id, '❌ Ошибка получения списка предупреждений.')

    def command_mutelist(self, peer_id, chat_id):
        try:
            users_with_mutes = self.db.get_users_with_active_mutes(chat_id)

            if not users_with_mutes:
                self.send_message(peer_id, '✅ Нет пользователей с блокировкой чата')
                return

            mutelist_text = "🔇 Список пользователей с запретом писать в чат:\n\n"

            for user_data in users_with_mutes:
                user_id = user_data['user_id']
                mute_until = user_data['mute_until']
                reason = user_data['reason'] if user_data['reason'] else 'Не указана'

                # Форматируем дату окончания мута
                if mute_until:
                    try:
                        mute_dt = datetime.fromisoformat(mute_until.replace('Z', '+00:00'))
                        months = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 
                                 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
                        mute_until_formatted = f"{mute_dt.day} {months[mute_dt.month - 1]} {mute_dt.year} в {mute_dt.strftime('%H:%M')}"
                    except:
                        mute_until_formatted = mute_until[:16].replace('T', ' ')
                else:
                    mute_until_formatted = 'Навсегда'

                mutelist_text += f"[id{user_id}|@id{user_id}]\n"
                mutelist_text += f"└ До: {mute_until_formatted}\n"
                mutelist_text += f"└ Причина: {reason}\n\n"

            mutelist_text += f"Всего пользователей в муте: {len(users_with_mutes)}"

            self.send_message(peer_id, mutelist_text)
        except Exception as e:
            self.log(f"Ошибка получения списка мутов: {e}")
            self.send_message(peer_id, '❌ Ошибка получения списка мутов.')

    def command_getban(self, peer_id, target_id, chat_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        try:
            ban = self.db.get_user_ban_in_chat(target_id, chat_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            if ban:
                banned_by_info = self.get_user_info(ban['banned_by'])
                banned_by_name = banned_by_info['screen_name'] if banned_by_info else str(ban['banned_by'])

                ban_text = f"""⛔️ Информация о блокировке:

☕️ Пользователь: @{target_name}
☕️ Причина: {ban['reason']}
☕️ Заблокировал: @{banned_by_name}
☕️ Дата: {ban['created_at'][:16]}"""
            else:
                ban_text = f"✅ @{target_name}(Пользователь) не заблокирован в этом чате."

            self.send_message(peer_id, ban_text)
        except Exception as e:
            self.log(f"Ошибка получения информации о бане: {e}")
            self.send_message(peer_id, '❌ Ошибка получения информации о бане.')

    def command_getnick(self, peer_id, target_id, chat_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
            return

        try:
            nickname = self.db.get_user_nickname(target_id, chat_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            if nickname:
                self.send_message(peer_id, f'👤 Никнейм @{target_name} в этом чате: {nickname}')
            else:
                self.send_message(peer_id, f'❌ У @{target_name}(Пользователя) нету никнейма в этом чате.')
        except Exception as e:
            self.log(f"Ошибка получения никнейма: {e}")
            self.send_message(peer_id, '❌ Ошибка получения никнейма.')

    def command_setnick(self, peer_id, sender_id, target_id, nickname, chat_id):
        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
            return

        if not self.has_permission(sender_id, None, '40', chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /setnick доступен с Модератора (40) и выше.')
            return

        # Проверка на пустой никнейм или отсутствие target_id
        if not target_id or not nickname or not nickname.strip():
            error_message = """☕️ Отказано! УВы не указали пользователя и никнейм.

☕️ Примеры использования:
/snick @user модератор
/snick @user Администрация"""
            self.send_message(peer_id, error_message)
            return

        try:
            self.db.set_user_nickname(target_id, nickname, chat_id, sender_id)

            # Формируем новое сообщение в стиле [id123|Пользователю] выдан новый никнейм: Nickname | 123
            success_message = f'✅ [id{target_id}|Пользователю] выдан новый никнейм: {nickname}.'

            self.send_message(peer_id, success_message)
            self.log(f"Пользователю {target_id} установлен никнейм '{nickname}' в чате {chat_id}")
        except Exception as e:
            self.log(f"Ошибка установки никнейма: {e}")
            self.send_message(peer_id, '❌ Ошибка установки никнейма.')

    def command_removenick(self, peer_id, sender_id, target_id, chat_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
            return

        if not self.has_permission(sender_id, None, '40', chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /removenick доступен с Модератора (40) и выше.')
            return

        try:
            self.db.remove_user_nickname(target_id, chat_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.send_message(peer_id, f'✅ Администратор удалил никнейм @{target_name}(пользователю) в этом чате.')
        except Exception as e:
            self.log(f"Ошибка удаления никнейма: {e}")
            self.send_message(peer_id, '❌ Ошибка удаления никнейма.')

    def command_nicknames(self, peer_id, chat_id):
        try:
            if not chat_id:
                self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
                return

            # Получаем всех пользователей с никнеймами в этом чате
            chat_users_with_nicks = self.db.get_all_users_with_nicknames(chat_id)

            if not chat_users_with_nicks:
                nicknames_text = "✄ Список пользователей с никнеймами:\n\n пользователей"
                self.send_message(peer_id, nicknames_text)
                return

            nicknames_text = "✄ Список пользователей с никнеймами:\n\n"

            for index, user in enumerate(chat_users_with_nicks, 1):
                user_info = self.get_user_info(user['user_id'])

                # Получаем полное имя пользователя
                if user_info:
                    full_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip()
                    if not full_name:
                        full_name = user_info.get('screen_name', str(user['user_id']))
                else:
                    full_name = str(user['user_id'])

                # Форматируем строку с номером, именем и никнеймом
                nicknames_text += f"{index}. [id{user['user_id']}|{full_name}] - {user['nickname']}\n"

            self.send_message(peer_id, nicknames_text)
        except Exception as e:
            self.log(f"Ошибка получения списка никнеймов: {e}")
            self.send_message(peer_id, '❌ Ошибка получения списка никнеймов.')

    def command_nonames(self, peer_id, chat_id):
        try:
            if not chat_id:
                self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
                return

            # Получаем всех пользователей без никнеймов в этом чате
            chat_users_without_nicks = self.db.get_all_users_without_nicknames(chat_id)

            nonames_text = f"""Пользователи без ников:

"""
            for user in chat_users_without_nicks[:20]:
                user_info = self.get_user_info(user['user_id'])
                username = user_info['screen_name'] if user_info else str(user['user_id'])
                nonames_text += f"🔸 @{username}\n"

            if not chat_users_without_nicks:
                nonames_text += "✅ Все пользователи этого чата имеют никнеймы\n"
            self.send_message(peer_id, nonames_text)
        except Exception as e:
            self.log(f"Ошибка получения списка без никнеймов: {e}")
            self.send_message(peer_id, '❌ Ошибка получения списка пользователей без никнеймов.')

    def command_zov(self, peer_id, sender_id, text, chat_id):
        if not self.has_permission(sender_id, None, 100, chat_id):
            self.send_message(peer_id, '❌ Только создатель может использовать эту команду.')
            return

        message = f"📢 Вызов!\n\n{text if text else 'Важное объявление!'}\n\n@all"
        self.send_message(peer_id, message)

    def command_reg(self, peer_id, target_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        try:
            user = self.db.get_user(target_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            if user and user['join_date']:
                reg_date = user['join_date'][:10]
                self.send_message(peer_id, f'📅 @{target_name} зарегистрирован: {reg_date}')
            else:
                self.send_message(peer_id, f'❌ Дата регистрации @{target_name} неизвестна.')
        except Exception as e:
            self.log(f"Ошибка получения даты регистрации: {e}")
            self.send_message(peer_id, '❌ Ошибка получения даты регистрации.')

    def command_checknicks(self, peer_id, sender_id):
        if not self.has_permission(sender_id, None, '100'):
            self.send_message(peer_id, '❌ Только создатель может использовать эту команду.')
            return

        self.send_message(peer_id, '🔄 Синхронизация никнеймов выполнена.')

    def command_broadcast(self, peer_id, sender_id, message_text):
        if not message_text:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать текст рассылки.')
            return

        sender_info = self.get_user_info(sender_id)
        sender_name = sender_info['screen_name'] if sender_info else str(sender_id)

        # Проверяем, является ли отправитель гранд-менеджером
        if sender_name != CONFIG['grand_manager']:
            self.send_message(peer_id, '❌ Только создатель может использовать рассылку!')
            return

        try:
            # Получаем список всех чатов из базы данных
            cursor = self.db.conn.cursor()
            cursor.execute('SELECT DISTINCT chat_id FROM chat_roles WHERE chat_id IS NOT NULL')
            chat_ids = cursor.fetchall()

            broadcast_message = f"📢 РАССЫЛКА\n\n{message_text}"
            sent_count = 0
            error_count = 0

            # Отправляем в групповые чаты
            for chat_row in chat_ids:
                chat_id = chat_row['chat_id']
                peer_id_to_send = chat_id + 2000000000
                try:
                    self.send_message(peer_id_to_send, broadcast_message)
                    sent_count += 1
                except Exception as e:
                    error_count += 1
                    self.log(f"Ошибка отправки в чат {chat_id}: {e}")

            # Отправляем в ЛС бота (используем ID создателя)
            try:
                self.send_message(sender_id, broadcast_message)
                sent_count += 1
            except Exception as e:
                error_count += 1
                self.log(f"Ошибка отправки в ЛС: {e}")

            result_message = f"✅ Рассылка завершена!\n📤 Отправлено: {sent_count}\n❌ Ошибок: {error_count}"
            self.send_message(peer_id, result_message)

        except Exception as e:
            self.log(f"Ошибка рассылки: {e}")
            self.send_message(peer_id, '❌ Ошибка при выполнении рассылки.')

    def command_chatinfo(self, peer_id, chat_id):
        try:
            response = self.api_request('messages.getConversationMembers', {
                'peer_id': peer_id
            })

            if not response:
                self.send_message(peer_id, '❌ Ошибка получения информации о чате.')
                return

            total_members = response.get('count', 0)
            online_count = 0

            for profile in response.get('profiles', []):
                if profile.get('online'):
                    online_count += 1

            chat_roles = self.db.get_all_chat_roles(chat_id) if chat_id else []
            admin_count = len([r for r in chat_roles if r['role_level'] >= 20])

            chatinfo_text = f"""ℹ️ Информация о чате:

🆔 ID чата: {chat_id if chat_id else 'Личные сообщения'}
👥 Участников: {total_members}
🟢 Онлайн: {online_count}
👑 Администрация: {admin_count}
🔴 Оффлайн: {total_members - online_count}"""
            self.send_message(peer_id, chatinfo_text)
        except Exception as e:
            self.log(f"Ошибка получения информации о чате: {e}")
            self.send_message(peer_id, '❌ Ошибка получения информации о чате.')

    def command_q(self, peer_id, user_id, chat_id):
        """Покинуть конференцию"""
        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
            return

        try:
            # Кикаем пользователя (он сам себя)
            kick_result = self.kick_user(chat_id, user_id, 'Вышел по команде /q')

            if kick_result:
                user_info = self.get_user_info(user_id)
                user_name = user_info['screen_name'] if user_info else str(user_id)

                self.send_message(peer_id, f'👋 [id{user_id}|{user_name}] покинул(а) конференцию.')
                self.log(f"Пользователь {user_name} вышел из чата {chat_id} по команде /q")
            else:
                self.send_message(peer_id, '❌ Не удалось покинуть конференцию. Попробуйте позже.')
        except Exception as e:
            self.log(f"Ошибка выхода из конференции: {e}")
            self.send_message(peer_id, '❌ Ошибка при выходе из конференции.')

    def command_chatid(self, peer_id, chat_id):
        """Показать ID конференции"""
        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
            return

        chatid_text = f"""🆔 Идентификатор конференции

ID: {chat_id}
Peer ID: {peer_id}"""
        self.send_message(peer_id, chatid_text)

    def command_editcmd(self, peer_id, sender_id, command, level, chat_id):
        """Изменить уровень доступа для команды"""
        if not self.has_permission(sender_id, None, 100, chat_id):
            self.send_message(peer_id, '❌ Только создатель может изменять права доступа к командам.')
            return

        # Убираем префикс если есть
        if command.startswith('/'):
            command = command[1:]

        try:
            level = int(level)
        except ValueError:
            self.send_message(peer_id, '❌ Приоритет должен быть числом.')
            return

        if level < 0 or level > 100:
            self.send_message(peer_id, '❌ Приоритет должен быть от 0 до 100.')
            return

        try:
            cursor = self.db.conn.cursor()

            # Сохраняем или обновляем настройку
            cursor.execute('''
                INSERT OR REPLACE INTO command_permissions 
                (chat_id, command, required_level, set_by, set_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (chat_id or 0, command, level, sender_id))

            self.db.conn.commit()

            self.send_message(peer_id, f'✅ Для команды /{command} установлен приоритет {level}')
            self.log(f"Команда /{command} теперь требует уровень {level}")
        except Exception as e:
            self.log(f"Ошибка изменения прав команды: {e}")
            self.send_message(peer_id, '❌ Ошибка при изменении прав команды.')

    def command_newpull(self, peer_id, sender_id, union_name, chat_id):
        """Создать объединение конференций"""
        if not union_name or not union_name.strip():
            self.send_message(peer_id, '❌ Использование: /newpull [название]')
            return

        if not self.has_permission(sender_id, None, 100, chat_id):
            self.send_message(peer_id, '❌ Только создатель может создавать объединения.')
            return

        try:
            import string
            # Генерируем уникальный ключ
            union_key = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

            cursor = self.db.conn.cursor()
            cursor.execute('''
                INSERT INTO chat_unions (union_key, union_name, created_by)
                VALUES (?, ?, ?)
            ''', (union_key, union_name, sender_id))

            self.db.conn.commit()

            result_text = f"""✅ Объединение создано!

📋 Название: {union_name}
🔑 Ключ: {union_key}

💡 Используйте /pull {union_key} в других конференциях для добавления их в объединение."""

            self.send_message(peer_id, result_text)
            self.log(f"Создано объединение '{union_name}' с ключом {union_key}")
        except Exception as e:
            self.log(f"Ошибка создания объединения: {e}")
            self.send_message(peer_id, '❌ Ошибка при создании объединения.')

    def command_pull(self, peer_id, sender_id, union_key, chat_id):
        """Добавить конференцию в объединение"""
        if not union_key or not union_key.strip():
            self.send_message(peer_id, '❌ Использование: /pull [ключ объединения]')
            return

        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
            return

        if not self.has_permission(sender_id, None, 100, chat_id):
            self.send_message(peer_id, '❌ Только создатель конференции (роль 100) может добавить её в объединение.')
            return

        try:
            cursor = self.db.conn.cursor()

            # Проверяем существование объединения
            cursor.execute('SELECT id, union_name FROM chat_unions WHERE union_key = ?', (union_key,))
            union = cursor.fetchone()

            if not union:
                self.send_message(peer_id, f'❌ Объединение с ключом "{union_key}" не найдено.')
                return

            union_id = union['id']
            union_name = union['union_name']

            # Проверяем, не добавлена ли уже конференция
            cursor.execute('SELECT id FROM union_chats WHERE union_id = ? AND chat_id = ?', 
                         (union_id, chat_id))

            if cursor.fetchone():
                self.send_message(peer_id, f'⚠️ Конференция уже добавлена в объединение "{union_name}".')
                return

            # Добавляем конференцию в объединение
            cursor.execute('''
                INSERT INTO union_chats (union_id, chat_id, added_by)
                VALUES (?, ?, ?)
            ''', (union_id, chat_id, sender_id))

            self.db.conn.commit()

            self.send_message(peer_id, f'✅ Конференция добавлена в объединение "{union_name}"!')
            self.log(f"Чат {chat_id} добавлен в объединение {union_name}")
        except Exception as e:
            self.log(f"Ошибка добавления в объединение: {e}")
            self.send_message(peer_id, '❌ Ошибка при добавлении в объединение.')

    def command_pullinfo(self, peer_id, sender_id, chat_id):
        """Просмотр информации об объединении конференции"""
        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
            return

        try:
            cursor = self.db.conn.cursor()

            # Получаем объединение для текущего чата
            cursor.execute('''
                SELECT cu.id, cu.union_key, cu.union_name, cu.created_by, cu.created_at
                FROM chat_unions cu
                JOIN union_chats uc ON cu.id = uc.union_id
                WHERE uc.chat_id = ?
            ''', (chat_id,))
            union = cursor.fetchone()

            if not union:
                self.send_message(peer_id, '❌ Этот чат не входит в объединение.')
                return

            # Получаем все чаты в объединении
            cursor.execute('''
                SELECT chat_id, added_by, added_at
                FROM union_chats 
                WHERE union_id = ?
                ORDER BY added_at ASC
            ''', (union['id'],))
            union_chats = cursor.fetchall()

            # Получаем информацию о создателе
            creator_info = self.get_user_info(union['created_by'])
            creator_name = creator_info['screen_name'] if creator_info else str(union['created_by'])

            info_text = f"""📋 ИНФОРМАЦИЯ ОБ ОБЪЕДИНЕНИИ

🏷 Название: {union['union_name']}
🔑 Ключ: {union['union_key']}
👤 Создатель: @{creator_name}
📅 Создано: {union['created_at'][:10]}

💬 Конференции в объединении ({len(union_chats)}):
"""

            for i, uc in enumerate(union_chats, 1):
                if uc['chat_id'] == chat_id:
                    info_text += f"{i}. Эта конференция (ID: {uc['chat_id']})\n"
                else:
                    info_text += f"{i}. Конференция ID: {uc['chat_id']}\n"

            self.send_message(peer_id, info_text)

        except Exception as e:
            self.log(f"Ошибка получения информации об объединении: {e}")
            self.send_message(peer_id, '❌ Ошибка при получении информации об объединении.')

    def command_pulldel(self, peer_id, sender_id, chat_id):
        """Удалить все конференции из объединения"""
        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
            return

        if not self.has_permission(sender_id, None, 100, chat_id):
            self.send_message(peer_id, '❌ Только создатель может удалять объединения.')
            return

        try:
            cursor = self.db.conn.cursor()

            # Получаем объединение для текущего чата
            cursor.execute('''
                SELECT cu.id, cu.union_name, cu.created_by
                FROM chat_unions cu
                JOIN union_chats uc ON cu.id = uc.union_id
                WHERE uc.chat_id = ?
            ''', (chat_id,))
            union = cursor.fetchone()

            if not union:
                self.send_message(peer_id, '❌ Этот чат не входит в объединение.')
                return

            # Проверяем, является ли пользователь создателем объединения
            if union['created_by'] != sender_id:
                self.send_message(peer_id, '❌ Только создатель объединения может удалить его.')
                return

            union_id = union['id']
            union_name = union['union_name']

            # Получаем количество чатов в объединении
            cursor.execute('SELECT COUNT(*) as count FROM union_chats WHERE union_id = ?', (union_id,))
            chats_count = cursor.fetchone()['count']

            # Удаляем все чаты из объединения
            cursor.execute('DELETE FROM union_chats WHERE union_id = ?', (union_id,))

            # Удаляем само объединение
            cursor.execute('DELETE FROM chat_unions WHERE id = ?', (union_id,))

            self.db.conn.commit()

            result_text = f"""✅ Объединение удалено!

📋 Название: {union_name}
💬 Удалено конференций: {chats_count}

ℹ️ Все конференции больше не связаны с объединением."""

            self.send_message(peer_id, result_text)
            self.log(f"Удалено объединение '{union_name}' с {chats_count} конференциями")

        except Exception as e:
            self.log(f"Ошибка удаления объединения: {e}")
            self.send_message(peer_id, '❌ Ошибка при удалении объединения.')

    def command_wipe(self, peer_id, sender_id, wipe_type, chat_id):
        """Очистить список предупреждений/блокировок/ролей/никнеймов"""
        if not self.has_permission(sender_id, None, 100, chat_id):
            self.send_message(peer_id, '❌ Только создатель может использовать команду /wipe.')
            return

        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
            return

        try:
            cursor = self.db.conn.cursor()

            if wipe_type == 'bans':
                # Очистить все баны в чате
                cursor.execute('UPDATE chat_bans SET is_active = 0 WHERE chat_id = ? AND is_active = 1', 
                             (chat_id,))
                count = cursor.rowcount
                self.send_message(peer_id, f'✅ Очищено блокировок: {count}')

            elif wipe_type == 'warn':
                # Очистить все предупреждения в чате
                cursor.execute('DELETE FROM warnings WHERE chat_id = ?', (chat_id,))
                count = cursor.rowcount
                self.send_message(peer_id, f'✅ Очищено предупреждений: {count}')

            elif wipe_type == 'nick':
                # Очистить все никнеймы в чате
                cursor.execute('UPDATE chat_nicknames SET is_active = 0 WHERE chat_id = ? AND is_active = 1', 
                             (chat_id,))
                count = cursor.rowcount
                self.send_message(peer_id, f'✅ Очищено никнеймов: {count}')

            elif wipe_type == 'roles':
                # Очистить все роли в чате (кроме создателя)
                cursor.execute('''
                    UPDATE chat_roles 
                    SET is_active = 0 
                    WHERE chat_id = ? AND role_level < 100 AND is_active = 1
                ''', (chat_id,))
                count = cursor.rowcount
                self.send_message(peer_id, f'✅ Очищено ролей: {count}')

            else:
                self.send_message(peer_id, '❌ Неверный тип. Доступные: bans, warn, nick, roles')
                return

            self.db.conn.commit()
            self.log(f"Выполнена очистка {wipe_type} в чате {chat_id}")

        except Exception as e:
            self.log(f"Ошибка очистки данных: {e}")
            self.send_message(peer_id, '❌ Ошибка при очистке данных.')

    def command_ai(self, peer_id, user_id, question):
        """Задать вопрос ChatGPT"""
        if not question or not question.strip():
            self.send_message(peer_id, '❌ Использование: /ai [ваш вопрос]')
            return

        try:
            # Отправляем уведомление что обрабатываем запрос
            self.send_message(peer_id, '🤖 Обрабатываю ваш запрос...')

            # Используем бесплатный API g4f
            api_url = "https://api.airforce/v1/chat/completions"

            headers = {
                "Content-Type": "application/json"
            }

            payload = {
                "model": "gpt-4o-mini",
                "messages": [
                    {
                        "role": "user",
                        "content": question
                    }
                ],
                "max_tokens": 1000
            }

            response = requests.post(api_url, json=payload, headers=headers, timeout=30)

            if response.status_code == 200:
                data = response.json()

                if 'choices' in data and len(data['choices']) > 0:
                    ai_response = data['choices'][0]['message']['content']

                    # Ограничиваем длину ответа (VK ограничивает длину сообщений)
                    max_length = 4000
                    if len(ai_response) > max_length:
                        ai_response = ai_response[:max_length] + "..."

                    response_text = f"🤖 AI:\n\n{ai_response}"
                    self.send_message(peer_id, response_text)
                else:
                    self.send_message(peer_id, '❌ Не удалось получить ответ от AI.')
            else:
                self.send_message(peer_id, f'❌ Ошибка API: {response.status_code}')

        except requests.Timeout:
            self.send_message(peer_id, '❌ Превышено время ожидания ответа. Попробуйте позже.')
        except Exception as e:
            self.log(f"Ошибка команды /ai: {e}")
            self.send_message(peer_id, '❌ Произошла ошибка при обработке запроса.')

    def command_piar(self, peer_id, sender_id, text, interval_minutes, chat_id):
        """Запустить периодическую рассылку сообщения в чате"""
        global PIAR_TIMERS

        if not self.has_permission(sender_id, None, 100, chat_id):
            self.send_message(peer_id, '❌ Только создатель может использовать команду /piar.')
            return

        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в групповых чатах.')
            return

        # Проверяем, есть ли уже активная рассылка
        timer_key = str(chat_id)
        if timer_key in PIAR_TIMERS:
            PIAR_TIMERS[timer_key].cancel()
            del PIAR_TIMERS[timer_key]

        if not text or not text.strip():
            # Остановка рассылки
            self.send_message(peer_id, '✅ Пиар-рассылка остановлена.')
            self.log(f"Пиар-рассылка остановлена в чате {chat_id}")
            return

        if interval_minutes < 5:
            self.send_message(peer_id, '❌ Минимальный интервал рассылки: 5 минут.')
            return

        if interval_minutes > 1440:  # 24 часа
            self.send_message(peer_id, '❌ Максимальный интервал рассылки: 1440 минут (24 часа).')
            return

        # Запускаем периодическую рассылку
        self.send_piar_message(peer_id, text, interval_minutes, chat_id)

        self.send_message(peer_id, f'✅ Пиар-рассылка запущена!\n📝 Текст: {text}\n⏱ Интервал: {interval_minutes} минут\n\n💡 Для остановки используйте: /piar стоп')
        self.log(f"Пиар-рассылка запущена в чате {chat_id} с интервалом {interval_minutes} минут")

    def send_piar_message(self, peer_id, text, interval_minutes, chat_id):
        """Отправляет пиар-сообщение и планирует следующую отправку"""
        global PIAR_TIMERS

        try:
            # Отправляем сообщение
            self.send_message(peer_id, f"📢 {text}")

            # Планируем следующую отправку
            timer_key = str(chat_id)
            interval_seconds = interval_minutes * 60

            timer = threading.Timer(
                interval_seconds,
                self.send_piar_message,
                args=[peer_id, text, interval_minutes, chat_id]
            )
            timer.start()
            PIAR_TIMERS[timer_key] = timer

        except Exception as e:
            self.log(f"Ошибка отправки пиар-сообщения: {e}")

    def command_moder(self, peer_id, sender_id, target_id, chat_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        # Проверка, что пользователь не пытается изменить свою собственную роль
        if target_id == sender_id:
            self.send_message(peer_id, '❌ Вы не можете изменять свою собственную роль.')
            return

        if not self.has_permission(sender_id, None, 60, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /moder доступен с Администратора (60) и выше.')
            return

        try:
            role_name = self.get_role_name_for_level(40, chat_id)
            self.db.set_chat_role(target_id, chat_id, 40, role_name, sender_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.send_message(peer_id, f'✅ @{target_name}(Пользователь) получил роль "{role_name}" (40).')
        except Exception as e:
            self.log(f"Ошибка назначения модератора: {e}")
            self.send_message(peer_id, '❌ Ошибка при назначении модератора.')

    def command_admin(self, peer_id, sender_id, target_id, chat_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        # Проверка, что пользователь не пытается изменить свою собственную роль
        if target_id == sender_id:
            self.send_message(peer_id, '❌ Вы не можете изменять свою собственную роль.')
            return

        if not self.has_permission(sender_id, None, 80, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /admin доступен с Спец.Администратора (80) и выше.')
            return

        # Проверка системного уровня доступа отправителя и цели
        sender_system_admin = self.db.get_system_admin(sender_id)
        target_system_admin = self.db.get_system_admin(target_id)

        sender_level = sender_system_admin['access_level'] if sender_system_admin else 0
        target_level = target_system_admin['access_level'] if target_system_admin else 0

        # Запретить изменение роли, если у цели уровень выше или равен отправителю
        if target_level >= sender_level and target_level > 0:
            self.send_message(peer_id, '❌ Вы не можете изменять роль пользователя с равным или более высоким системным уровнем доступа.')
            return

        try:
            role_name = self.get_role_name_for_level(60, chat_id)
            self.db.set_chat_role(target_id, chat_id, 60, role_name, sender_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.send_message(peer_id, f'✅ @{target_name}(Пользователь) получил роль "{role_name}" (60).')
        except Exception as e:
            self.log(f"Ошибка назначения администратора: {e}")
            self.send_message(peer_id, '❌ Ошибка при назначении администратора.')

    def command_owner(self, peer_id, sender_id, target_id, chat_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        # Проверка, что пользователь не пытается изменить свою собственную роль
        if target_id == sender_id:
            self.send_message(peer_id, '❌ Вы не можете изменять свою собственную роль.')
            return

        if not self.has_permission(sender_id, None, 100, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Только создатель чата может использовать команду /owner.')
            return

        try:
            role_name = self.get_role_name_for_level(100, chat_id)
            self.db.set_chat_role(target_id, chat_id, 100, role_name, sender_id)
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.send_message(peer_id, f'👑 @{target_name}(Пользователю) передана роль "{role_name}" (100).')
        except Exception as e:
            self.log(f"Ошибка передачи прав владельца: {e}")
            self.send_message(peer_id, '❌ Ошибка при передаче прав владельца.')

    def command_removerole(self, peer_id, sender_id, target_id, chat_id):
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать пользователя.')
            return

        # Проверка, что пользователь не пытается изменить свою собственную роль
        if target_id == sender_id:
            self.send_message(peer_id, '❌ Вы не можете изменять свою собственную роль.')
            return

        # Проверяем права - модератор и выше
        if not self.has_permission(sender_id, None, 40, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /removerole доступен с Модератора (40) и выше.')
            return

        # Проверяем, что отправитель может снять роль у целевого пользователя
        sender_role = self.get_user_role(sender_id, chat_id)
        target_role = self.get_user_role(target_id, chat_id)

        # Только владелец (уровень 100) может снимать роли с тех, кто на его уровне или выше
        if sender_role['level'] < 100 and target_role['level'] >= sender_role['level']:
            self.send_message(peer_id, f'⛔️ Отказано! Вы не можете снять роль у пользователя с уровнем {target_role["level"]} ({target_role["name"]}), так как ваш уровень {sender_role["level"]} ({sender_role["name"]}).')
            return

        try:
            self.db.remove_chat_role(target_id, chat_id)

            # Получаем никнеймы для отображения
            sender_nick = self.db.get_user_nickname(sender_id, chat_id) if chat_id else None
            target_nick = self.db.get_user_nickname(target_id, chat_id) if chat_id else None

            sender_info = self.get_user_info(sender_id)
            target_info = self.get_user_info(target_id)

            sender_display = sender_nick if sender_nick else (sender_info['screen_name'] if sender_info else str(sender_id))
            target_display = target_nick if target_nick else (target_info['screen_name'] if target_info else str(target_id))

            self.send_message(peer_id, f'✅ [id{sender_id}|{sender_display}] забрал права [id{target_id}|{target_display}].')
        except Exception as e:
            self.log(f"Ошибка снятия роли: {e}")
            self.send_message(peer_id, '❌ Ошибка при снятии роли.')

    def command_delete(self, peer_id, sender_id, message, chat_id):
        """Удалить сообщение"""
        # Проверяем права доступа (уровень 40+ - администратор)
        if not self.has_permission(sender_id, None, '40', chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /delete доступен с Модератора (40) и выше.')
            return

        # Проверяем, что команда используется в ответ на сообщение
        if 'reply_message' not in message or not message['reply_message']:
            self.send_message(peer_id, '❌ Используйте команду /delete в ответ на сообщение, которое нужно удалить.')
            return

        try:
            # Получаем ID сообщения для удаления
            message_id = message['reply_message']['conversation_message_id']

            # Удаляем сообщение через VK API
            params = {
                'peer_id': peer_id,
                'cmids': [message_id],
                'delete_for_all': 1
            }

            response = self.api_request('messages.delete', params)

            if response:
                self.send_message(peer_id, '🗑️ Сообщение успешно удалено!')
                self.log(f"Сообщение {message_id} удалено в чате {chat_id}")
            else:
                self.send_message(peer_id, '❌ Ошибка при удалении сообщения.')

        except Exception as e:
            self.log(f"Ошибка удаления сообщения: {e}")
            self.send_message(peer_id, '❌ Ошибка при удалении сообщения.')

    def command_gkick(self, peer_id, sender_id, target_id, reason, chat_id):
        """Исключить пользователя из всех конференций объединения"""
        if not target_id:
            error_message = """⛔️ Отказано! Вы не указали пользователя для исключения пользователя из всех конференций.

☕️ Примеры:
/gkick @durov причина
/gkick - ответом на сообщение"""
            self.send_message(peer_id, error_message)
            return

        if not self.has_permission(sender_id, None, 80, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /gkick доступен с Спец.Администратора (80) и выше.')
            return

        if not reason or not reason.strip():
            reason = 'Нарушение правил'

        try:
            # Получаем объединение для текущего чата
            cursor = self.db.conn.cursor()
            cursor.execute('''
                SELECT cu.id, cu.union_name 
                FROM chat_unions cu
                JOIN union_chats uc ON cu.id = uc.union_id
                WHERE uc.chat_id = ?
            ''', (chat_id,))
            union = cursor.fetchone()

            if not union:
                self.send_message(peer_id, '❌ Этот чат не входит в объединение.')
                return

            # Получаем все чаты в объединении
            cursor.execute('''
                SELECT chat_id FROM union_chats WHERE union_id = ?
            ''', (union['id'],))
            union_chats = cursor.fetchall()

            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            success_count = 0
            for chat in union_chats:
                try:
                    chat_peer_id = chat['chat_id'] + 2000000000
                    if self.kick_user(chat['chat_id'], target_id, reason):
                        success_count += 1
                except Exception as e:
                    self.log(f"Ошибка кика из чата {chat['chat_id']}: {e}")

            result_text = f'✅ Глобальный кик @{target_name} выполнен!\n'
            result_text += f'📊 Исключен из {success_count}/{len(union_chats)} конференций объединения "{union["union_name"]}".\n'
            result_text += f'📝 Причина: {reason}'

            self.send_message(peer_id, result_text)
            self.log(f"Глобальный кик пользователя {target_name} из {success_count} чатов. Причина: {reason}")

        except Exception as e:
            self.log(f"Ошибка глобального кика: {e}")
            self.send_message(peer_id, '❌ Ошибка при выполнении глобального кика.')

    def command_logs(self, peer_id, chat_id, page=1):
        """Просмотреть информацию о действиях пользователей в конференции"""
        try:
            logs_text = f"📋 ЛОГИ ДЕЙСТВИЙ В ЧАТЕ\n\n"
            logs_text += "Функция логирования в разработке.\n"
            logs_text += f"Страница: {page}"

            self.send_message(peer_id, logs_text)
        except Exception as e:
            self.log(f"Ошибка получения логов: {e}")
            self.send_message(peer_id, '❌ Ошибка получения логов.')

    def command_gsetnick(self, peer_id, sender_id, target_id, nickname, chat_id):
        """Установить никнейм пользователю во всех конференциях объединения"""
        if not target_id or not nickname:
            self.send_message(peer_id, '⛔️ Отказано! Необходимо указать пользователя и никнейм.')
            return

        try:
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.send_message(peer_id, f'✅ Глобальный никнейм "{nickname}" установлен для @{target_name} во всех конференциях объединения.')
            self.log(f"Глобальный никнейм установлен: {target_name} -> {nickname}")

        except Exception as e:
            self.log(f"Ошибка установки глобального никнейма: {e}")
            self.send_message(peer_id, '❌ Ошибка при установке глобального никнейма.')

    def command_gremovenick(self, peer_id, sender_id, target_id, chat_id):
        """Удалить никнейм пользователю во всех конференциях объединения"""
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Необходимо указать пользователя.')
            return

        try:
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.send_message(peer_id, f'✅ Глобальный никнейм удален для @{target_name} во всех конференциях объединения.')
            self.log(f"Глобальный никнейм удален: {target_name}")

        except Exception as e:
            self.log(f"Ошибка удаления глобального никнейма: {e}")
            self.send_message(peer_id, '❌ Ошибка при удалении глобального никнейма.')

    def command_gzov(self, peer_id, sender_id, message_text, chat_id):
        """Упомянуть всех пользователей во всех конференциях объединения"""
        try:
            self.send_message(peer_id, f'📢 Глобальный зов выполняется...\n{message_text if message_text else ""}')
            self.log(f"Глобальный зов выполнен в чате {chat_id}")

        except Exception as e:
            self.log(f"Ошибка глобального зова: {e}")
            self.send_message(peer_id, '❌ Ошибка при выполнении глобального зова.')

    def command_gban(self, peer_id, sender_id, target_id, reason, chat_id):
        """Заблокировать пользователя во всех конференциях объединения"""
        if not target_id:
            error_message = """⛔️ Отказано! Вы не указали пользователя для блокировки пользователя из всех конференций.

🍹Примеры:
/gban @durov причина
/gban - ответом на сообщение"""
            self.send_message(peer_id, error_message)
            return

        if not self.has_permission(sender_id, None, 80, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /gban доступен с Спец.Администратора (80) и выше.')
            return

        if not reason or not reason.strip():
            reason = 'Нарушение правил'

        try:
            # Получаем объединение для текущего чата
            cursor = self.db.conn.cursor()
            cursor.execute('''
                SELECT cu.id, cu.union_name 
                FROM chat_unions cu
                JOIN union_chats uc ON cu.id = uc.union_id
                WHERE uc.chat_id = ?
            ''', (chat_id,))
            union = cursor.fetchone()

            if not union:
                self.send_message(peer_id, '❌ Этот чат не входит в объединение.')
                return

            # Получаем все чаты в объединении
            cursor.execute('''
                SELECT chat_id FROM union_chats WHERE union_id = ?
            ''', (union['id'],))
            union_chats = cursor.fetchall()

            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            success_count = 0
            for chat in union_chats:
                try:
                    # Добавляем бан в каждый чат
                    self.db.add_chat_ban(target_id, chat['chat_id'], reason, sender_id)
                    # Кикаем из чата
                    if self.kick_user(chat['chat_id'], target_id, reason):
                        success_count += 1
                except Exception as e:
                    self.log(f"Ошибка бана в чате {chat['chat_id']}: {e}")

            result_text = f'🚫 Глобальная блокировка @{target_name} выполнена!\n'
            result_text += f'📊 Заблокирован в {success_count}/{len(union_chats)} конференциях объединения "{union["union_name"]}".\n'
            result_text += f'📝 Причина: {reason}'

            self.send_message(peer_id, result_text)
            self.log(f"Глобальная блокировка пользователя {target_name} в {success_count} чатах. Причина: {reason}")

        except Exception as e:
            self.log(f"Ошибка глобальной блокировки: {e}")
            self.send_message(peer_id, '❌ Ошибка при выполнении глобальной блокировки.')

    def command_gunban(self, peer_id, sender_id, target_id, chat_id):
        """Снять блокировку с пользователя во всех конференциях объединения"""
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Необходимо указать пользователя.')
            return

        if not self.has_permission(sender_id, None, 80, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /gunban доступен с Спец.Администратора (80) и выше.')
            return

        try:
            # Получаем объединение для текущего чата
            cursor = self.db.conn.cursor()
            cursor.execute('''
                SELECT cu.id, cu.union_name 
                FROM chat_unions cu
                JOIN union_chats uc ON cu.id = uc.union_id
                WHERE uc.chat_id = ?
            ''', (chat_id,))
            union = cursor.fetchone()

            if not union:
                self.send_message(peer_id, '❌ Этот чат не входит в объединение.')
                return

            # Получаем все чаты в объединении
            cursor.execute('''
                SELECT chat_id FROM union_chats WHERE union_id = ?
            ''', (union['id'],))
            union_chats = cursor.fetchall()

            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            success_count = 0
            for chat in union_chats:
                try:
                    # Снимаем бан в каждом чате
                    self.db.remove_chat_ban(target_id, chat['chat_id'])
                    success_count += 1
                except Exception as e:
                    self.log(f"Ошибка разбана в чате {chat['chat_id']}: {e}")

            result_text = f'✅ Глобальная разблокировка @{target_name} выполнена!\n'
            result_text += f'📊 Разблокирован в {success_count}/{len(union_chats)} конференциях объединения "{union["union_name"]}".'

            self.send_message(peer_id, result_text)
            self.log(f"Глобальная разблокировка пользователя {target_name} в {success_count} чатах")

        except Exception as e:
            self.log(f"Ошибка глобальной разблокировки: {e}")
            self.send_message(peer_id, '❌ Ошибка при выполнении глобальной разблокировки.')

    def command_filter(self, peer_id, sender_id, args, chat_id):
        """Управление запрещёнными словами в конференции"""
        if not chat_id:
            self.send_message(peer_id, '❌ Команда доступна только в беседах.')
            return

        if not self.has_permission(sender_id, None, 40, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /filter доступен с Администратора (40) и выше.')
            return

        try:
            # Если команда без аргументов - показываем справку
            if len(args) < 2:
                filter_text = "📋 ФИЛЬТР ЗАПРЕЩЁННЫХ СЛОВ\n\n"
                filter_text += "Использование:\n"
                filter_text += "/filter добавить [слово] — добавить запрещённое слово\n"
                filter_text += "/filter удалить [слово] — удалить из списка\n"
                filter_text += "/filter список (или list) — показать все запрещённые слова\n"
                self.send_message(peer_id, filter_text)
                return

            action = args[1].lower()

            # Добавление запрещенного слова
            if action in ['добавить', 'add']:
                word = ' '.join(args[2:]) if len(args) > 2 else None
                if not word:
                    self.send_message(peer_id, '❌ Укажите слово для добавления.\n💡 Пример: /filter добавить блять')
                    return

                if self.db.add_filtered_word(chat_id, word, sender_id):
                    self.send_message(peer_id, f'✅ Слово "{word}" добавлено в список запрещённых.')
                    self.log(f"Добавлено запрещённое слово: {word} в чате {chat_id}")
                else:
                    self.send_message(peer_id, f'⚠️ Слово "{word}" уже есть в списке запрещённых.')

            # Удаление запрещенного слова
            elif action in ['удалить', 'remove', 'delete']:
                word = ' '.join(args[2:]) if len(args) > 2 else None
                if not word:
                    self.send_message(peer_id, '❌ Укажите слово для удаления.\n💡 Пример: /filter удалить блять')
                    return

                if self.db.remove_filtered_word(chat_id, word):
                    self.send_message(peer_id, f'✅ Слово "{word}" удалено из списка запрещённых.')
                    self.log(f"Удалено запрещённое слово: {word} из чата {chat_id}")
                else:
                    self.send_message(peer_id, f'⚠️ Слово "{word}" не найдено в списке запрещённых.')

            # Показать список запрещенных слов
            elif action in ['список', 'list']:
                words = self.db.get_filtered_words(chat_id)
                if words:
                    filter_text = "📋 Список запрещенных слов:\n\n"
                    for i, word in enumerate(words, 1):
                        filter_text += f"{i}. {word}\n"
                    filter_text += f"\n📝 Всего: {len(words)} слов"
                else:
                    filter_text = "📋 Список запрещенных слов:\n\n"
                    filter_text += "✅ Список пуст. Запрещённые слова не добавлены.\n\n"
                    filter_text += "💡 Чтобы добавить слово, используйте:\n/filter добавить [слово]"
                self.send_message(peer_id, filter_text)

            # Неизвестное действие
            else:
                filter_text = "❌ Неизвестное действие.\n\n"
                filter_text += "Доступные команды:\n"
                filter_text += "/filter добавить [слово]\n"
                filter_text += "/filter удалить [слово]\n"
                filter_text += "/filter список"
                self.send_message(peer_id, filter_text)

        except Exception as e:
            self.log(f"Ошибка управления фильтром: {e}")
            self.send_message(peer_id, '❌ Ошибка управления фильтром.')

    def command_settings(self, peer_id, sender_id, chat_id):
        """Изменить настройки конференции"""
        try:
            settings_text = "⚙️ НАСТРОЙКИ КОНФЕРЕНЦИИ\n\n"
            settings_text += "Функция настроек в разработке.\n"
            settings_text += "\nДоступные параметры:\n"
            settings_text += "• Автомодерация\n"
            settings_text += "• Приветствие\n"
            settings_text += "• Фильтр слов\n"
            settings_text += "• Логирование\n"

            self.send_message(peer_id, settings_text)
        except Exception as e:
            self.log(f"Ошибка настроек: {e}")
            self.send_message(peer_id, '❌ Ошибка настроек.')

    def command_rr(self, peer_id, sender_id, target_id, chat_id):
        """Снять с пользователя все права во всех конференциях объединения"""
        if not target_id:
            self.send_message(peer_id, '⛔️ Отказано! Необходимо указать пользователя.')
            return

        try:
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.send_message(peer_id, f'✅ Глобальное снятие прав с @{target_name} выполняется во всех конференциях объединения...')
            self.log(f"Глобальное снятие прав с {target_name}")

        except Exception as e:
            self.log(f"Ошибка глобального снятия прав: {e}")
            self.send_message(peer_id, '❌ Ошибка при глобальном снятии прав.')

    def command_gnewrole(self, peer_id, sender_id, role_level, role_name, chat_id):
        """Создать роль во всех конференциях объединения"""
        if not role_level or not role_name:
            self.send_message(peer_id, '⛔️ Отказано! Использование: /gnewrole [приоритет] [название]')
            return

        if not self.has_permission(sender_id, None, 80, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /gnewrole доступен с Спец.Администратора (80) и выше.')
            return

        try:
            role_level = int(role_level)
        except ValueError:
            self.send_message(peer_id, '❌ Уровень роли должен быть числом.')
            return

        if role_level < 0 or role_level > 100:
            self.send_message(peer_id, '⛔️ Отказано! Приоритет роли должен быть от 0 до 100.')
            return

        try:
            # Получаем объединение для текущего чата
            cursor = self.db.conn.cursor()
            cursor.execute('''
                SELECT cu.id, cu.union_name 
                FROM chat_unions cu
                JOIN union_chats uc ON cu.id = uc.union_id
                WHERE uc.chat_id = ?
            ''', (chat_id,))
            union = cursor.fetchone()

            if not union:
                self.send_message(peer_id, '❌ Этот чат не входит в объединение.')
                return

            # Получаем все чаты в объединении
            cursor.execute('''
                SELECT chat_id FROM union_chats WHERE union_id = ?
            ''', (union['id'],))
            union_chats = cursor.fetchall()

            success_count = 0
            is_system_role = role_level in CONFIG['roles']

            for chat in union_chats:
                try:
                    # Создаем или обновляем кастомную роль в каждом чате
                    cursor.execute('''
                        INSERT OR REPLACE INTO custom_role_definitions 
                        (chat_id, role_level, role_name, created_by, created_at, is_active)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, 1)
                    ''', (chat['chat_id'], role_level, role_name, sender_id))

                    # Обновляем название роли у всех пользователей в этом чате
                    cursor.execute('''
                        UPDATE chat_roles 
                        SET role_name = ?
                        WHERE chat_id = ? AND role_level = ? AND is_active = 1
                    ''', (role_name, chat['chat_id'], role_level))

                    success_count += 1
                except Exception as e:
                    self.log(f"Ошибка создания роли в чате {chat['chat_id']}: {e}")

            self.db.conn.commit()

            result_text = f'✅ Глобальное создание роли "{role_name}" ({role_level}) выполнено!\n'
            result_text += f'📊 Роль создана в {success_count}/{len(union_chats)} конференциях объединения "{union["union_name"]}".'

            self.send_message(peer_id, result_text)
            self.log(f"Глобальное создание роли '{role_name}' ({role_level}) в {success_count} чатах")

        except Exception as e:
            self.log(f"Ошибка глобального создания роли: {e}")
            self.send_message(peer_id, '❌ Ошибка при глобальном создании роли.')

    def command_gsetrole(self, peer_id, sender_id, target_id, role_level, chat_id):
        """Выдать пользователю роль во всех конференциях объединения"""
        if not target_id or not role_level:
            help_text = """☕️ Аргументы введены неверно. Необходимо указать пользователя и роль.

☕️ Примеры использования:
/gsetrole @user 40
/gsetrole @user Модератор"""
            self.send_message(peer_id, help_text)
            return

        if not self.has_permission(sender_id, None, 80, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /gsetrole доступен с Спец.Администратора (80) и выше.')
            return

        try:
            # Получаем объединение для текущего чата
            cursor = self.db.conn.cursor()
            cursor.execute('''
                SELECT cu.id, cu.union_name 
                FROM chat_unions cu
                JOIN union_chats uc ON cu.id = uc.union_id
                WHERE uc.chat_id = ?
            ''', (chat_id,))
            union = cursor.fetchone()

            if not union:
                self.send_message(peer_id, '❌ Этот чат не входит в объединение.')
                return

            # Получаем все чаты в объединении
            cursor.execute('''
                SELECT chat_id FROM union_chats WHERE union_id = ?
            ''', (union['id'],))
            union_chats = cursor.fetchall()

            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            # Парсим уровень роли
            try:
                role_level_num = int(role_level)
            except ValueError:
                # Ищем роль по названию
                role_level_num = None
                for level, name in CONFIG['roles'].items():
                    if name.lower() == role_level.lower():
                        role_level_num = level
                        break

                if not role_level_num:
                    self.send_message(peer_id, f'⛔️ Отказано! Роль "{role_level}" не найдена.')
                    return

            role_level = role_level_num

            success_count = 0
            for chat in union_chats:
                try:
                    # Получаем название роли для этого чата
                    role_name = self.get_role_name_for_level(role_level, chat['chat_id'])
                    # Устанавливаем роль в каждом чате
                    self.db.set_chat_role(target_id, chat['chat_id'], role_level, role_name, sender_id)
                    success_count += 1
                except Exception as e:
                    self.log(f"Ошибка установки роли в чате {chat['chat_id']}: {e}")

            result_text = f'✅ Глобальная выдача роли {role_level} для @{target_name} выполнена!\n'
            result_text += f'📊 Роль выдана в {success_count}/{len(union_chats)} конференциях объединения "{union["union_name"]}".'

            self.send_message(peer_id, result_text)
            self.log(f"Глобальная выдача роли {role_level} для {target_name} в {success_count} чатах")

        except Exception as e:
            self.log(f"Ошибка глобальной выдачи роли: {e}")
            self.send_message(peer_id, '❌ Ошибка при глобальной выдаче роли.')

    def command_role(self, peer_id, sender_id, target_id, role_level, chat_id):
        """Назначить роль пользователю"""
        if not target_id or not role_level:
            help_text = """☕️ Отказано! Что бы назначить роль укажите пользователя и роль.

❓ Примеры использования:
 /role @user 80
 /role @user Модератор"""
            self.send_message(peer_id, help_text)
            return

        # Проверка, что пользователь не пытается изменить свою собственную роль
        if target_id == sender_id:
            self.send_message(peer_id, '❌ Вы не можете изменять свою собственную роль.')
            return

        # Получаем роли до парсинга
        sender_role = self.get_user_role(sender_id, chat_id)
        target_role = self.get_user_role(target_id, chat_id)

        # Проверяем права - модератор и выше
        if not self.has_permission(sender_id, None, 40, chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /role доступен с Модератора (40) и выше.')
            return

        # Пытаемся преобразовать role_level в число
        role_level_num = None
        role_name_input = None

        try:
            role_level_num = int(role_level)
        except ValueError:
            # Если не число, то это может быть название роли
            role_name_input = role_level

            # Ищем роль по названию в стандартных ролях
            for level, name in CONFIG['roles'].items():
                if name.lower() == role_name_input.lower():
                    role_level_num = level
                    break

            # Если не нашли в стандартных, ищем в системных ролях
            if role_level_num is None:
                for level, name in CONFIG['system_roles'].items():
                    if name.lower() == role_name_input.lower():
                        role_level_num = level
                        break

            # Если не нашли в системных, ищем в кастомных ролях чата
            if not role_level_num and chat_id:
                cursor = self.db.conn.cursor()
                cursor.execute(
                    'SELECT role_level FROM custom_role_definitions WHERE chat_id = ? AND LOWER(role_name) = ? AND is_active = 1 LIMIT 1',
                    (chat_id, role_name_input.lower())
                )
                custom_role = cursor.fetchone()
                if custom_role:
                    role_level_num = custom_role['role_level']

            if not role_level_num:
                self.send_message(peer_id, f'⛔️ Отказано! Роль "{role_name_input}" не найдена.')
                return

        role_level = role_level_num

        # Проверка: нельзя изменять роль пользователя с уровнем выше или равным своему
        if sender_role['level'] < 100 and target_role['level'] >= sender_role['level']:
            self.send_message(peer_id, f'⛔️ Отказано! Вы не можете изменять роль пользователя с уровнем {target_role["level"]} ({target_role["name"]}), так как ваш уровень {sender_role["level"]} ({sender_role["name"]}).')
            return

        # Проверка: нельзя назначить роль выше или равную своей (кроме владельца уровня 100)
        if sender_role['level'] < 100 and role_level >= sender_role['level']:
            self.send_message(peer_id, f'⛔️ Отказано! Вы не можете назначить роль с приоритетом {role_level} или выше вашей ({sender_role["level"]}).')
            return

        try:
            # Проверяем, существует ли такая роль
            if not self.role_exists(role_level, chat_id):
                self.send_message(peer_id, f'❌ Роль с уровнем {role_level} не существует.')
                return

            # Получаем название роли
            role_name = self.get_role_name_for_level(role_level, chat_id)

            # Устанавливаем роль пользователю в чате
            self.db.set_chat_role(target_id, chat_id, role_level, role_name, sender_id)

            target_display = self.get_display_name(target_id, chat_id)

            # Указываем, что роль назначена в конкретном чате
            self.send_message(peer_id, f'✅ [id{target_id}|{target_display}] назначена роль «{role_name}» с приорететом {role_level}.')
            self.log(f"Пользователю {target_display} назначена роль '{role_name}' [{role_level}]")

        except Exception as e:
            self.log(f"Ошибка назначения роли: {e}")
            self.send_message(peer_id, '❌ Ошибка при назначении роли.')

    def command_delrole(self, peer_id, sender_id, role_level, chat_id):
        """Сбросить роль в беседе в исходное состояние"""
        if not role_level:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать уровень роли.')
            return

        if not self.has_permission(sender_id, None, '60', chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /delrole доступен с Администратора (60) и выше.')
            return

        try:
            role_level = int(role_level)
        except ValueError:
            self.send_message(peer_id, '⛔️ Отказано! Урововень роли должен быть числом.')
            return

        if role_level <= 0:
            self.send_message(peer_id, '⛔️ Отказано! Нельзя сбросить базовую роль "Пользователь".')
            return

        # Проверяем, это системная роль или кастомная
        is_system_role = role_level in CONFIG['roles']

        try:
            cursor = self.db.conn.cursor()

            # Проверяем, существует ли определение кастомной роли
            cursor.execute(
                'SELECT role_name FROM custom_role_definitions WHERE chat_id = ? AND role_level = ? AND is_active = 1',
                (chat_id, role_level)
            )
            custom_role_def = cursor.fetchone()

            # Получаем всех пользователей с этой ролью в чате
            cursor.execute(
                'SELECT user_id, role_name FROM chat_roles WHERE chat_id = ? AND role_level = ? AND is_active = 1',
                (chat_id, role_level)
            )
            users_with_role = cursor.fetchall()

            # Если нет ни определения роли, ни пользователей с этой ролью
            if not custom_role_def and not users_with_role and not is_system_role:
                self.send_message(peer_id, f'⛔️ Отказано! Роль с уровнем {role_level} не найдена в этом чате.')
                return

            # Получаем название роли
            if custom_role_def:
                role_name = custom_role_def['role_name']
            elif users_with_role:
                role_name = users_with_role[0]['role_name']
            elif is_system_role:
                role_name = CONFIG['roles'][role_level]
            else:
                role_name = f'Роль уровня {role_level}'

            if is_system_role:
                # Для системных ролей восстанавливаем оригинальное название
                original_name = CONFIG['roles'][role_level]

                # Деактивируем определение кастомной роли (если было изменено название)
                cursor.execute(
                    'UPDATE custom_role_definitions SET is_active = 0 WHERE chat_id = ? AND role_level = ? AND is_active = 1',
                    (chat_id, role_level)
                )

                # Обновляем название роли у всех пользователей на оригинальное
                cursor.execute('''
                    UPDATE chat_roles 
                    SET role_name = ?
                    WHERE chat_id = ? AND role_level = ? AND is_active = 1
                ''', (original_name, chat_id, role_level))

                self.db.conn.commit()

                affected_count = len(users_with_role)
                if affected_count > 0:
                    self.send_message(peer_id, f'✅ Роль с приорететом [{role_level}] восстановлена на "{original_name}".')
                else:
                    self.send_message(peer_id, f'✅ Кастомное название системной роли [{role_level}] удалено. Роль восстановлена на "{original_name}".')
            else:
                # Для кастомных ролей удаляем роль и сбрасываем пользователей
                # Деактивируем роль для всех пользователей
                cursor.execute(
                    'UPDATE chat_roles SET is_active = 0 WHERE chat_id = ? AND role_level = ? AND is_active = 1',
                    (chat_id, role_level)
                )

                # Деактивируем определение кастомной роли
                cursor.execute(
                    'UPDATE custom_role_definitions SET is_active = 0 WHERE chat_id = ? AND role_level = ? AND is_active = 1',
                    (chat_id, role_level)
                )

                # Устанавливаем всем пользователям роль "Пользователь" (уровень 0)
                for user_row in users_with_role:
                    cursor.execute('''
                        INSERT OR REPLACE INTO chat_roles 
                        (user_id, chat_id, role_level, role_name, granted_by, granted_at, is_active)
                        VALUES (?, ?, 0, 'Пользователь', ?, CURRENT_TIMESTAMP, 1)
                    ''', (user_row['user_id'], chat_id, sender_id))

                self.db.conn.commit()

                affected_count = len(users_with_role)
                if affected_count > 0:
                    self.send_message(peer_id, f'✅ Роль "{role_name}" ({role_level}) удалена.')
                else:
                    self.send_message(peer_id, f'✅ Роль "{role_name}" ({role_level}) удалена.')

        except Exception as e:
            self.log(f"Ошибка сброса роли: {e}")
            self.send_message(peer_id, '❌ Ошибка при сбросе роли.')

    def command_gdelrole(self, peer_id, sender_id, role_level):
        """Сбросить роль глобально в исходное состояние"""
        if not role_level:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно! Необходимо указать приоретет роли.')
            return

        if not self.has_permission(sender_id, None, '60'):
            self.send_message(peer_id, '')
            return

        try:
            role_level = int(role_level)
        except ValueError:
            self.send_message(peer_id, '⛔️ Отказано! Приоретет роли должен быть числом.')
            return

        # Проверяем, что это не системная роль
        if role_level in CONFIG['roles']:
            self.send_message(peer_id, f'❌ Нельзя сбросить системную роль уровня {role_level}.')
            return

        try:
            cursor = self.db.conn.cursor()

            # Получаем всех пользователей с этой ролью во всех чатах
            cursor.execute(
                'SELECT user_id, chat_id, role_name FROM chat_roles WHERE role_level = ? AND is_active = 1',
                (role_level,)
            )
            users_with_role = cursor.fetchall()

            # Сбрасываем роль для всех пользователей во всех чатах
            cursor.execute(
                'UPDATE chat_roles SET is_active = 0 WHERE role_level = ? AND is_active = 1',
                (role_level,)
            )

            # Устанавливаем всем пользователям роль "Пользователь" (уровень 0)
            for user_row in users_with_role:
                cursor.execute('''
                    INSERT OR REPLACE INTO chat_roles 
                    (user_id, chat_id, role_level, role_name, granted_by, granted_at, is_active)
                    VALUES (?, ?, 0, 'Пользователь', ?, CURRENT_TIMESTAMP, 1)
                ''', (user_row['user_id'], user_row['chat_id'], sender_id))

            # Деактивируем все определения кастомных ролей с этим уровнем
            cursor.execute(
                'UPDATE custom_role_definitions SET is_active = 0 WHERE role_level = ? AND is_active = 1',
                (role_level,)
            )

            self.db.conn.commit()

            affected_count = len(users_with_role)
            self.send_message(peer_id, f'✅ Роль уровня {role_level} удалена глобально.')

        except Exception as e:
            self.log(f"Ошибка сброса глобальной роли: {e}")
            self.send_message(peer_id, '❌ Ошибка при сбросе глобальной роли.')

    def command_welcome(self, peer_id, sender_id, text, chat_id):
        """Установить приветственное сообщение"""
        if not text:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать текст приветствия.')
            return

        if not self.has_permission(sender_id, None, '80', chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /welcome доступен с роли (80) и выше')
            return

        self.send_message(peer_id, f'✅ Приветственное сообщение установлено:\n\n{text}')

    def command_setrules(self, peer_id, sender_id, text, chat_id):
        """Установить правила чата"""
        if not text:
            self.send_message(peer_id, '❌ Использование: /setrules [текст правил]')
            return

        if not self.has_permission(sender_id, None, '80', chat_id):
            self.send_message(peer_id, '❌ Только руководители могут устанавливать правила.')
            return

        self.send_message(peer_id, f'✅ Правила чата обновлены:\n\n{text}')

    def command_inactive(self, peer_id, sender_id, days, chat_id):
        """Исключить неактивных пользователей"""
        if not days:
            self.send_message(peer_id, '⛔️ Отказано! Аргументы введены неверно. Необходимо указать количество дней.')
            return

        if not self.has_permission(sender_id, None, '80', chat_id):
            self.send_message(peer_id, '❌ Только руководители могут исключать неактивных.')
            return

        try:
            days = int(days)
        except ValueError:
            self.send_message(peer_id, '❌ Количество дней должно быть числом.')
            return

        self.send_message(peer_id, f'🔍 Поиск пользователей, неактивных более {days} дней...\n\n⚠️ Функция в разработке.')

    def command_initadmin(self, peer_id, sender_id):
        """Инициализировать администрацию"""
        if not self.has_permission(sender_id, None, '100'):
            self.send_message(peer_id, '❌ Только создатель может инициализировать администрацию.')
            return

        self.send_message(peer_id, '⚙️ Администрация системы инициализирована.')

    def command_pin(self, peer_id, sender_id, message, chat_id):
        """Закрепить сообщение"""
        # Проверяем права доступа (уровень 80+ - руководитель)
        if not self.has_permission(sender_id, None, '80', chat_id):
            self.send_message(peer_id, '⛔️ Отказано! Доступ к команде /pin дсотупен с роли Спец.Администратора (80) и выше.')
            return

        # Проверяем, что команда используется в ответ на сообщение
        if 'reply_message' not in message or not message['reply_message']:
            self.send_message(peer_id, '❌ Используйте команду /pin в ответ на сообщение, которое нужно закрепить.')
            return

        try:
            # Получаем ID сообщения для закрепления
            message_id = message['reply_message']['conversation_message_id']

            # Закрепляем сообщение через VK API
            params = {
                'peer_id': peer_id,
                'conversation_message_id': message_id
            }

            response = self.api_request('messages.pin', params)

            if response:
                self.send_message(peer_id, '📌 Сообщение успешно закреплено!')
                self.log(f"Сообщение {message_id} закреплено в чате {chat_id}")
            else:
                self.send_message(peer_id, '❌ Ошибка при закреплении сообщения.')

        except Exception as e:
            self.log(f"Ошибка закрепления сообщения: {e}")
            self.send_message(peer_id, '❌ Ошибка при закреплении сообщения.')

    def command_unpin(self, peer_id, sender_id, chat_id):
        """Открепить сообщение"""
        # Проверяем права доступа (уровень 80+ - руководитель)
        if not self.has_permission(sender_id, None, '80', chat_id):
            self.send_message(peer_id, '❌ Только руководители и выше могут открреплять сообщения.')
            return

        try:
            # Открепляем сообщение через VK API
            params = {
                'peer_id': peer_id
            }

            response = self.api_request('messages.unpin', params)

            if response:
                self.send_message(peer_id, '📌 Закрепленное сообщение удалено!')
                self.log(f"Сообщение откреплено в чате {chat_id}")
            else:
                self.send_message(peer_id, '❌ Ошибка при откреплении сообщения или сообщение не было закреплено.')

        except Exception as e:
            self.log(f"Ошибка открепления сообщения: {e}")
            self.send_message(peer_id, '❌ Ошибка при открреплении сообщения.')

    def command_roulette(self, peer_id, sender_id):
        roulette_help = """🎰 КАЗИНО РУЛЕТКА

🎲 Доступные ставки:
🔢 /ставка чет [сумма] четные числа (x2)
🔢 /ставка нечет [сумма] нечетные числа (x2)
🔴 /ставка красное [сумма] красные числа (x2)
⚫ /ставка черное [сумма] черные числа (x2)
🎯 /ставка [число] [сумма] конкретное число (x36)

💰 Пример: /ставка красное 1000
🎯 Пример: /ставка 7 500

🔴 Красные: 1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36
⚫ Черные: 2,4,6,8,10,11,13,15,17,20,22,24,26,28,29,31,33,35
🟢 Зеленое: 0 (банк забирает все ставки)"""
        self.send_message(peer_id, roulette_help)

    def command_bet(self, peer_id, sender_id, bet_type, bet_amount, bet_target=None, chat_id=None):
        global ROULETTE_TIMERS

        try:
            bet_amount = int(bet_amount)
        except (ValueError, TypeError):
            self.send_message(peer_id, '❌ Сумма ставки должна быть числом!')
            return

        if bet_amount < 100:
            self.send_message(peer_id, '❌ Минимальная ставка: 100 монет!')
            return

        if bet_amount > 1000000000:
            self.send_message(peer_id, '❌ Максимальная ставка: 1,000,000,000 монет!')
            return

        # Проверяем баланс
        if not self.db.can_afford_bet(sender_id, bet_amount):
            balance = self.db.get_user_balance(sender_id)
            self.send_message(peer_id, f'❌ Недостаточно средств! Ваш баланс: {balance["balance"]:,} $')
            return

        # Списываем ставку
        self.db.update_user_balance(sender_id, -bet_amount)

        # Получаем или создаем активную игру
        active_game = self.db.get_active_roulette_game(chat_id or peer_id)
        if not active_game:
            game_id = self.db.create_roulette_game(chat_id or peer_id)
            # При создании новой игры отправляем уведомление
            self.send_message(peer_id, '🎰 Игра "Рулетка" началась!\n⏱️ Приём ставок в течение 5 секунд...')
        else:
            game_id = active_game['id']

        # Получаем информацию о пользователе
        sender_info = self.get_user_info(sender_id)
        sender_name = sender_info['screen_name'] if sender_info else str(sender_id)

        # Добавляем ставку в базу
        display_bet_type = bet_target if bet_target else bet_type
        self.db.add_roulette_bet(game_id, sender_id, sender_name, bet_type, bet_target, bet_amount)

        # Отправляем подтверждение ставки в красивом формате
        bet_confirmation = f"✅ [id{sender_id}|{sender_name}] — {bet_amount:,} $ на {display_bet_type}"
        self.send_message(peer_id, bet_confirmation)

        # Сбрасываем предыдущий таймер если он был
        timer_key = str(chat_id or peer_id)
        if timer_key in ROULETTE_TIMERS:
            ROULETTE_TIMERS[timer_key].cancel()

        # Запускаем новый таймер на 5 секунд
        timer = threading.Timer(5.0, self.end_roulette_round, args=[peer_id, game_id, chat_id or peer_id])
        timer.start()
        ROULETTE_TIMERS[timer_key] = timer

    def end_roulette_round(self, peer_id, game_id, chat_id):
        global ROULETTE_TIMERS

        # Удаляем таймер из словаря
        timer_key = str(chat_id)
        if timer_key in ROULETTE_TIMERS:
            del ROULETTE_TIMERS[timer_key]

        # Получаем все ставки перед закрытием
        bets = self.db.get_game_bets(game_id)

        if not bets:
            self.send_message(peer_id, '🎰 Игра отменена - нет ставок.')
            self.db.end_roulette_game(game_id, -1)
            return

        # Отправляем сообщение о закрытии ставок с эмодзи рулетки
        self.send_message(peer_id, '🎰 Приём ставок для игры "Рулетка" закрыт.\n⏱️ Итоги раунда через 5 секунд...')

        # Ждем 5 секунд
        time.sleep(5)

        # Генерируем результат
        winning_number = random.randint(0, 36)

        # Определяем цвет
        red_numbers = [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36]

        if winning_number == 0:
            color_emoji = "🟢"
            color_name = "зеленое"
        elif winning_number in red_numbers:
            color_emoji = "🔴"
            color_name = "красное"
        else:
            color_emoji = "⚫"
            color_name = "черное"

        # Завершаем игру
        self.db.end_roulette_game(game_id, winning_number)

        # Определяем победителей и формируем результат
        winners = []
        total_lost = 0
        all_bets_info = []

        for bet in bets:
            is_winner = False
            multiplier = 0

            if bet['bet_type'] == 'число' and bet['bet_target'] and int(bet['bet_target']) == winning_number:
                is_winner = True
                multiplier = 36
            elif bet['bet_type'] == 'чет' and winning_number > 0 and winning_number % 2 == 0:
                is_winner = True
                multiplier = 2
            elif bet['bet_type'] == 'нечет' and winning_number > 0 and winning_number % 2 == 1:
                is_winner = True
                multiplier = 2
            elif bet['bet_type'] == 'красное' and winning_number in red_numbers:
                is_winner = True
                multiplier = 2
            elif bet['bet_type'] == 'черное' and winning_number != 0 and winning_number not in red_numbers:
                is_winner = True
                multiplier = 2

            all_bets_info.append({
                'user_id': bet['user_id'],
                'username': bet['username'],
                'bet_amount': bet['bet_amount'],
                'bet_type': bet['bet_target'] if bet['bet_target'] else bet['bet_type'],
                'is_winner': is_winner,
                'multiplier': multiplier
            })

            if is_winner:
                win_amount = bet['bet_amount'] * multiplier
                self.db.update_user_balance(bet['user_id'], win_amount)
                winners.append({
                    'user_id': bet['user_id'],
                    'username': bet['username'],
                    'bet_amount': bet['bet_amount'],
                    'win_amount': win_amount,
                    'bet_type': bet['bet_target'] if bet['bet_target'] else bet['bet_type']
                })
            else:
                total_lost += bet['bet_amount']

        # Формируем итоговое сообщение в стиле как на фото
        result_text = f"🎰 Итоги игры \"Рулетка\":\n\n🎲 Выпало: {color_emoji} {winning_number}\n\n"

        if winners:
            for winner in winners:
                result_text += f"✅ [id{winner['user_id']}|{winner['username']}] выиграл {winner['win_amount']:,} $ (ставка {winner['bet_amount']:,} $ на {winner['bet_type']})\n"
        else:
            result_text += "❌ Все ставки проиграли!\n\n"

        if total_lost > 0:
            result_text += f"\n💰 Проиграно: {total_lost:,} $"

        self.send_message(peer_id, result_text)

    def command_sysadmins(self, peer_id, sender_id):
        """Показать список системных администраторов"""
        # Проверяем уровень доступа пользователя (1+ - агент поддержки и выше)
        system_admin = self.db.get_system_admin(sender_id)
        if not system_admin or system_admin['access_level'] < 1:
            self.send_message(peer_id, '❌ У вас нет прав для просмотра списка администрации.')
            return

        try:
            admins = self.db.get_all_system_admins()

            if not admins:
                self.send_message(peer_id, '❌ Нет системных администраторов.')
                return

            admin_text = "🛡️ АДМИНИСТРАЦИЯ БОТА\n\n"

            # Используем system_roles из CONFIG
            for level in sorted(CONFIG['system_roles'].keys(), reverse=True):
                level_admins = [admin for admin in admins if admin['access_level'] == level]
                if level_admins:
                    admin_text += f"👑 {CONFIG['system_roles'][level]} ({level})\n"
                    for admin in level_admins:
                        admin_text += f"   👤 [id{admin['user_id']}|@{admin['username']}]\n"
                    admin_text += "\n"

            self.send_message(peer_id, admin_text)

        except Exception as e:
            self.log(f"Ошибка получения списка системных администраторов: {e}")
            self.send_message(peer_id, '❌ Ошибка получения списка администрации.')

    def command_giveagent(self, peer_id, sender_id, target_id):
        """Выдать права агента поддержки"""
        if not target_id:
            self.send_message(peer_id, '❌ Использование: /giveagent [ID]')
            return

        system_admin = self.db.get_system_admin(sender_id)
        if not system_admin or system_admin['access_level'] < 2:
            self.send_message(peer_id, '❌ Только администраторы бота и выше могут выдавать права агентов.')
            return

        try:
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.db.add_system_admin(target_id, target_name, 1, sender_id)
            self.send_message(peer_id, f'✅ @{target_name}(Пользователю) выданы права агента поддержки.')

        except Exception as e:
            self.log(f"Ошибка выдачи прав агента: {e}")
            self.send_message(peer_id, '❌ Ошибка при выдаче прав агента.')

    def command_giveadm(self, peer_id, sender_id, target_id):
        """Выдать права администратора бота"""
        if not target_id:
            self.send_message(peer_id, '❌ Использование: /giveadm [ID]')
            return

        system_admin = self.db.get_system_admin(sender_id)
        if not system_admin or system_admin['access_level'] < 3:
            self.send_message(peer_id, '❌ Только зам.основатели и выше могут выдавать права администраторов.')
            return

        try:
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.db.add_system_admin(target_id, target_name, 2, sender_id)
            self.send_message(peer_id, f'✅ Пользователю @{target_name} выданы права администратора бота.')

        except Exception as e:
            self.log(f"Ошибка выдачи прав администратора: {e}")
            self.send_message(peer_id, '❌ Ошибка при выдаче прав администратора.')

    def command_givezam(self, peer_id, sender_id, target_id):
        """Выдать права зам.основателя бота"""
        if not target_id:
            self.send_message(peer_id, '❌ Использование: /givezam [ID]')
            return

        system_admin = self.db.get_system_admin(sender_id)
        if not system_admin or system_admin['access_level'] < 4:
            self.send_message(peer_id, '❌ Только основатель может выдавать права Зам.Основателя.')
            return

        try:
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.db.add_system_admin(target_id, target_name, 3, sender_id)
            self.send_message(peer_id, f'✅ Пользователю @{target_name} выданы права Зам.Основателя бота.')

        except Exception as e:
            self.log(f"Ошибка выдачи прав зам.основателя: {e}")
            self.send_message(peer_id, '❌ Ошибка при выдаче прав зам.основателя.')

def command_giveowner(self, peer_id, sender_id, target_id):
    """Выдать права основателя бота"""
    if not target_id:
        self.send_message(peer_id, '❌ Использование: /giveowner [ID]')
        return

    # Проверка через системные роли ИЛИ через конфиг разработчика
    system_admin = self.db.get_system_admin(sender_id)
    is_developer_from_config = (sender_id == GRAND_DEVELOPER_ID or 
                               self.get_user_info(sender_id) and 
                               self.get_user_info(sender_id).get('screen_name') == GRAND_DEVELOPER_USERNAME)
    
    if not system_admin and not is_developer_from_config:
        self.send_message(peer_id, '❌ Только разработчик может выдавать права основателя.')
        return

    # Если пользователь - разработчик из конфига, но нет в БД, добавляем его
    if is_developer_from_config and not system_admin:
        self.db.add_system_admin(sender_id, GRAND_DEVELOPER_USERNAME, 5, sender_id)
        self.log(f"Автоматически добавлен разработчик из конфига: {GRAND_DEVELOPER_USERNAME}")

    try:
        target_info = self.get_user_info(target_id)
        target_name = target_info['screen_name'] if target_info else str(target_id)

        self.db.add_system_admin(target_id, target_name, 4, sender_id)
        self.send_message(peer_id, f'✅ Пользователю @{target_name} выданы права основателя бота.')

    except Exception as e:
        self.log(f"Ошибка выдачи прав основателя: {e}")
        self.send_message(peer_id, '❌ Ошибка при выдаче прав основателя.')
        
    def command_null(self, peer_id, sender_id, target_id):
        """Снять системные права"""
        if not target_id:
            self.send_message(peer_id, '❌ Использование: /null [ID]')
            return

        system_admin = self.db.get_system_admin(sender_id)
        if not system_admin or system_admin['access_level'] < 2:
            self.send_message(peer_id, '❌ Только администраторы бота и выше могут снимать права.')
            return

        try:
            target_admin = self.db.get_system_admin(target_id)
            if not target_admin:
                self.send_message(peer_id, '❌ Пользователь не имеет системных прав.')
                return

            # Проверяем, что администратор не пытается снять права у вышестоящего
            if target_admin['access_level'] >= system_admin['access_level']:
                self.send_message(peer_id, '❌ Вы не можете снимать права у администраторов вашего уровня или выше.')
                return

            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.db.remove_system_admin(target_id)
            self.send_message(peer_id, f'✅ Системные права сняты с @{target_name}.')

        except Exception as e:
            self.log(f"Ошибка снятия системных прав: {e}")
            self.send_message(peer_id, '❌ Ошибка при снятии прав.')

    def command_sysban(self, peer_id, sender_id, target_id, days, reason):
        """Системный бан пользователя"""
        if not target_id or not reason:
            self.send_message(peer_id, '❌ Использование: /sysban [ID] [дни] [причина]')
            return

        system_admin = self.db.get_system_admin(sender_id)
        if not system_admin or system_admin['access_level'] < 2:
            self.send_message(peer_id, '❌ Только администраторы бота и выше могут выдавать системные баны.')
            return

        try:
            days_int = int(days) if days != '0' else None
        except ValueError:
            self.send_message(peer_id, '❌ Количество дней должно быть числом (0 = навсегда).')
            return

        try:
            # Проверяем, что пользователь не пытается забанить админа выше себя
            target_admin = self.db.get_system_admin(target_id)
            if target_admin and target_admin['access_level'] >= system_admin['access_level']:
                self.send_message(peer_id, '❌ Вы не можете банить администраторов вашего уровня или выше.')
                return

            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.db.add_system_ban(target_id, reason, sender_id, days_int)

            duration_text = f"на {days_int} дн." if days_int else "навсегда"
            self.send_message(peer_id, f'⛔️ Пользователь @{target_name} получил системный бан {duration_text}.\n📝 Причина: {reason}')

        except Exception as e:
            self.log(f"Ошибка системного бана: {e}")
            self.send_message(peer_id, '❌ Ошибка при выдаче системного бана.')

    def command_unsysban(self, peer_id, sender_id, target_id):
        """Снять системный бан"""
        if not target_id:
            self.send_message(peer_id, '❌ Использование: /unsysban [ID]')
            return

        system_admin = self.db.get_system_admin(sender_id)
        if not system_admin or system_admin['access_level'] < 2:
            self.send_message(peer_id, '❌ Только администраторы бота и выше могут снимать системные баны.')
            return

        try:
            ban = self.db.get_system_ban(target_id)
            if not ban:
                self.send_message(peer_id, '❌ Пользователь не имеет активного системного бана.')
                return

            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.db.remove_system_ban(target_id)
            self.send_message(peer_id, f'✅ Системный бан снят с @{target_name}.')

        except Exception as e:
            self.log(f"Ошибка снятия системного бана: {e}")
            self.send_message(peer_id, '❌ Ошибка при снятии бана.')

    def command_sysrole(self, peer_id, sender_id, target_id, role_level, chat_id):
        """Выдать системную роль"""
        if not target_id or not role_level:
            self.send_message(peer_id, '❌ Использование: /sysrole [ID] [уровень]')
            return

        system_admin = self.db.get_system_admin(sender_id)
        if not system_admin or system_admin['access_level'] < 2:
            self.send_message(peer_id, '❌ Только администраторы бота и выше могут выдавать системные роли.')
            return

        try:
            role_level = int(role_level)
        except ValueError:
            self.send_message(peer_id, '❌ Уровень роли должен быть числом.')
            return

        if role_level == 0:
            role_name = 'Пользователь'
        elif role_level in CONFIG['roles']:
            role_name = CONFIG['roles'][role_level]
        else:
            self.send_message(peer_id, f'❌ Роль с уровнем {role_level} не существует посмотрите достцпные роли в ролях.')
            return

        try:
            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            if chat_id:
                self.db.set_chat_role(target_id, chat_id, role_level, role_name, sender_id)
                context = f"в чате {chat_id}"
            else:
                self.db.create_or_update_user(target_id, target_name, None, role_level)
                context = "глобально"

            self.send_message(peer_id, f'✅ @{target_name}(Пользователю) выдана роль «{role_name}» {role_level}) {context}.')

        except Exception as e:
            self.log(f"Ошибка выдачи системной роли: {e}")
            self.send_message(peer_id, '❌ Ошибка при выдаче роли.')

    def command_dice(self, peer_id, sender_id, args, chat_id):
        global DICE_TIMERS

        if not args:
            # Показать доступные игры
            try:
                active_games = self.db.get_active_dice_games(chat_id or peer_id)

                if not active_games:
                    dice_help = """🎲 ИГРА В КОСТИ

📋 Доступные команды:
🎲 /кости [сумма] - игра на 2 игрока
🎲 /кости 3 [сумма] - игра на 3 игрока  
🎲 /кости 4 [сумма] - игра на 4 игрока

💰 Минимальная ставка: 100$
🎯 Побеждает тот, у кого больше очков на кубике

❌ Нет активных игр в кости"""
                    self.send_message(peer_id, dice_help)
                    return

                games_text = "🎲 АКТИВНЫЕ ИГРЫ В КОСТИ\n\n"
                for game in active_games:
                    players_count = self.db.get_dice_players_count(game['id'])
                    amount_display = self.format_number(game['bet_amount'])

                    games_text += f"🎮 Игра в кости №{game['id']}\n"
                    games_text += f"💰 Ставка: {amount_display}$\n"
                    games_text += f"👥 Мест: [{players_count}/{game['max_players']}]\n"
                    games_text += f"👤 Создатель: @{game['creator_username']}\n\n"

                games_text += "💡 Используйте /кости [номер] для подключения к игре"
                self.send_message(peer_id, games_text)

            except Exception as e:
                self.log(f"Ошибка показа активных игр в кости: {e}")
                self.send_message(peer_id, '❌ Ошибка получения списка игр.')
            return

        # Парсинг аргументов
        max_players = 2
        bet_amount_str = args[0]

        # Проверяем, первый аргумент - количество игроков или ставка
        if args[0] in ['3', '4']:
            max_players = int(args[0])
            if len(args) < 2:
                self.send_message(peer_id, f'❌ Использование: /кости {max_players} [сумма]')
                return
            bet_amount_str = args[1]
        elif len(args) == 1 and args[0].isdigit() and int(args[0]) <= 10:
            # Подключение к существующей игре
            try:
                game_id = int(args[0])
                game = self.db.get_dice_game(game_id)

                if not game:
                    self.send_message(peer_id, '❌ Игра не найдена!')
                    return

                if game['status'] != 'waiting':
                    self.send_message(peer_id, '❌ Игра уже завершена или отменена!')
                    return

                if game['creator_id'] == sender_id:
                    self.send_message(peer_id, '❌ Создатель игры не может играть в собственной игре!')
                    return

                if self.db.is_user_in_dice_game(game_id, sender_id):
                    self.send_message(peer_id, '❌ Вы уже участвуете в этой игре!')
                    return

                players_count = self.db.get_dice_players_count(game_id)
                if players_count >= game['max_players']:
                    self.send_message(peer_id, '❌ В игре нет свободных мест!')
                    return

                # Проверяем баланс
                bet_amount = game['bet_amount']
                if not self.db.can_afford_bet(sender_id, bet_amount):
                    balance = self.db.get_user_balance(sender_id)
                    self.send_message(peer_id, f'❌ Недостаточно средств! Ваш баланс: {balance["balance"]:,}$')
                    return

                # Списываем ставку и добавляем игрока
                self.db.update_user_balance(sender_id, -bet_amount)

                sender_info = self.get_user_info(sender_id)
                sender_name = sender_info['screen_name'] if sender_info else str(sender_id)

                self.db.join_dice_game(game_id, sender_id, sender_name)

                new_players_count = players_count + 1

                if new_players_count >= game['max_players']:
                    # Игра полная, запускаем
                    self.start_dice_game(peer_id, game_id, chat_id or peer_id)
                else:
                    # Обновляем информацию об игре
                    players = self.db.get_dice_players(game_id)
                    amount_display = self.format_number(bet_amount)

                    game_text = f"🎮 Игра в кости №{game_id}\n"
                    game_text += f"💰 Ставка: {amount_display}$\n"
                    game_text += f"👥 Мест: [{new_players_count}/{game['max_players']}]\n"
                    game_text += f"👤 Игроки:\n"

                    # Добавляем создателя
                    game_text += f"@{game['creator_username']} 🥷🏻\n"

                    # Добавляем других игроков
                    for player in players:
                        game_text += f"@{player['username']} 🎯\n"

                    game_text += f"\n⚠️ Внимание! Игра в кости будет отменена, если не будет завершена в течение 30 минут."

                    self.send_message(peer_id, game_text)

                return

            except ValueError:
                pass  # Не номер игры, продолжаем как обычная команда создания

        # Парсинг суммы ставки
        balance_data = self.db.get_user_balance(sender_id)
        user_balance = balance_data['balance']
        bet_amount = self.parse_amount(bet_amount_str, user_balance)

        if bet_amount is None or bet_amount <= 0:
            self.send_message(peer_id, '❌ Неверная сумма ставки!')
            return

        if bet_amount < 100:
            self.send_message(peer_id, '❌ Минимальная ставка: 100$!')
            return

        if bet_amount > user_balance:
            self.send_message(peer_id, f'❌ Недостаточно средств! Ваш баланс: {user_balance:,}$')
            return

        # Проверяем количество активных игр (максимум 5)
        active_games = self.db.get_active_dice_games(chat_id or peer_id, 5)
        if len(active_games) >= 5:
            self.send_message(peer_id, '❌ Максимальное количество активных игр в кости: 5!')
            return

        # Списываем ставку создателя
        self.db.update_user_balance(sender_id, -bet_amount)

        # Создаем игру
        sender_info = self.get_user_info(sender_id)
        sender_name = sender_info['screen_name'] if sender_info else str(sender_id)

        game_id = self.db.create_dice_game(chat_id or peer_id, sender_id, sender_name, bet_amount, max_players)

        # Форматируем сумму для отображения
        amount_display = self.format_number(bet_amount)

        # Создаем сообщение об игре
        game_text = f"🎮 Игра в кости №{game_id}\n"
        game_text += f"💰 Ставка: {amount_display}$\n"
        game_text += f"👥 Мест: [1/{max_players}]\n"
        game_text += f"👤 Игроки:\n"
        game_text += f"@{sender_name} 🥷🏻\n\n"
        game_text += f"⚠️ Внимание! Игра в кости будет отменена, если не будет завершена в течение 30 минут."

        # Создаем клавиатуру
        keyboard = self.create_dice_keyboard(game_id, is_creator=False)

        self.send_message(peer_id, game_text, keyboard)

        # Запускаем таймер на 30 минут
        timer = threading.Timer(1800.0, self.cancel_dice_game_timeout, args=[peer_id, game_id, chat_id or peer_id])
        timer.start()
        DICE_TIMERS[game_id] = timer

    def start_dice_game(self, peer_id, game_id, chat_id):
        """Запускает игру в кости когда набрались все игроки"""
        global DICE_TIMERS

        # Отменяем таймер
        if game_id in DICE_TIMERS:
            DICE_TIMERS[game_id].cancel()
            del DICE_TIMERS[game_id]

        try:
            game = self.db.get_dice_game(game_id)
            players = self.db.get_dice_players(game_id)

            if not game or not players:
                return

            # Бросаем кости для всех игроков
            results = []
            for player in players:
                dice_result = random.randint(1, 6)
                self.db.set_dice_result(game_id, player['user_id'], dice_result)
                results.append({
                    'user_id': player['user_id'],
                    'username': player['username'],
                    'dice_result': dice_result
                })

            # Добавляем создателя
            creator_dice = random.randint(1, 6)
            results.append({
                'user_id': game['creator_id'],
                'username': game['creator_username'],
                'dice_result': creator_dice
            })

            # Определяем победителя
            max_result = max(results, key=lambda x: x['dice_result'])
            winners = [r for r in results if r['dice_result'] == max_result['dice_result']]

            # Если ничья, переигрываем
            if len(winners) > 1:
                result_text = f"🎮 Игра в кости №{game_id}\n"
                for result in results:
                    result_text += f"@{result['username']}: {result['dice_result']}\n"
                result_text += f"\n🔄 Ничья! Переигровка...\n"

                self.send_message(peer_id, result_text)

                # Перезапускаем через 3 секунды
                timer = threading.Timer(3.0, self.start_dice_game, args=[peer_id, game_id, chat_id])
                timer.start()
                return

            # Определяем победителя
            winner = winners[0]
            total_bank = game['bet_amount'] * (len(players) + 1)  # +1 за создателя

            # Выплачиваем выигрыш
            self.db.update_user_balance(winner['user_id'], total_bank)

            # Завершаем игру
            self.db.end_dice_game(game_id, winner['user_id'])

            # Отправляем результат
            result_text = f"🎮 Игра в кости №{game_id}\n"
            for result in results:
                result_text += f"@{result['username']}: {result['dice_result']}\n"

            bank_display = self.format_number(total_bank)
            result_text += f"\n🏆 Победитель @{winner['username']}, он забирает весь банк {bank_display}$"

            self.send_message(peer_id, result_text)

        except Exception as e:
            self.log(f"Ошибка запуска игры в кости: {e}")

    def cancel_dice_game_timeout(self, peer_id, game_id, chat_id):
        """Отменяет игру по таймауту"""
        global DICE_TIMERS

        if game_id in DICE_TIMERS:
            del DICE_TIMERS[game_id]

        try:
            game = self.db.get_dice_game(game_id)
            players = self.db.get_dice_players(game_id)

            if game and game['status'] == 'waiting':
                # Возвращаем ставки
                self.db.update_user_balance(game['creator_id'], game['bet_amount'])

                for player in players:
                    self.db.update_user_balance(player['user_id'], game['bet_amount'])

                # Отменяем игру
                self.db.cancel_dice_game(game_id)

                # Отменяем таймер
                if game_id in DICE_TIMERS:
                    DICE_TIMERS[game_id].cancel()
                    del DICE_TIMERS[game_id]

                amount_display = self.format_number(game['bet_amount'])
                self.send_message(peer_id, f'⏰ Игра в кости №{game_id} отменена по таймауту. Ставки ({amount_display}$) возвращены всем участникам.')

        except Exception as e:
            self.log(f"Ошибка отмены игры по таймауту: {e}")

    def format_number(self, number):
        """Форматирует число для красивого отображения"""
        if number >= 1000000:
            return f"{number/1000000:.3f}".rstrip('0').rstrip('.') + "кк"
        elif number >= 1000:
            return f"{number/1000:.3f}".rstrip('0').rstrip('.') + "к"
        else:
            return f"{number:,}"

    def command_bonus(self, peer_id, sender_id):
        try:
            if not self.db.can_claim_bonus(sender_id):
                balance_data = self.db.get_user_balance(sender_id)
                last_claim_time_data = balance_data.get('last_bonus_claim')

                if last_claim_time_data:
                    try:
                        if isinstance(last_claim_time_data, str):
                            last_claim_time = datetime.fromisoformat(last_claim_time_data.replace('Z', '+00:00'))
                        else:
                            last_claim_time = last_claim_time_data

                        time_until_next_claim = (last_claim_time + timedelta(hours=1)) - datetime.now()

                        if time_until_next_claim.total_seconds() > 0:
                            hours, remainder = divmod(time_until_next_claim.total_seconds(), 3600)
                            minutes, seconds = divmod(remainder, 60)
                            time_left = f"{int(hours)}ч {int(minutes)}м" if hours > 0 else f"{int(minutes)}м {int(seconds)}с"
                            self.send_message(peer_id, f'❌ [id{sender_id}|Вы] уже получили бонус сегодня!\n⏰ Следующий бонус будет доступен через {time_left}')
                            return
                    except:
                        pass
                else:
                    self.send_message(peer_id, '⏰ Бонус можно получать только раз в час! Попробуйте позже.')
                    return

            # Генерируем случайный бонус от 1,000,000 до 5,000,000
            bonus_amount = random.randint(1000000, 5000000)

            self.db.update_user_balance(sender_id, bonus_amount)
            self.db.claim_bonus(sender_id)

            sender_info = self.get_user_info(sender_id)
            sender_name = sender_info['screen_name'] if sender_info else str(sender_id)

            new_balance = self.db.get_user_balance(sender_id)

            bonus_text = f"""🎁 Бонус:

🎉 Поздравляем, @{sender_name}!
💰 Вы получили: {bonus_amount:,} $

💳 Ваш баланс: {new_balance['balance']:,} $

⏰ Следующий бонус через: 1 час"""

            self.send_message(peer_id, bonus_text)

        except Exception as e:
            self.log(f"Ошибка выдачи бонуса: {e}")
            self.send_message(peer_id, '❌ Ошибка при получении бонуса.')

    def parse_amount(self, amount_str, user_balance):
        """Парсит сумму с поддержкой сокращений к, м и 'все'"""
        if not amount_str:
            return None

        amount_str = amount_str.lower().replace(',', '').replace('.', '').replace(' ', '')

        if amount_str == 'все':
            return user_balance

        if amount_str.endswith('к'):
            try:
                return int(float(amount_str[:-1]) * 1000)
            except:
                return None
        elif amount_str.endswith('м'):
            try:
                return int(float(amount_str[:-1]) * 1000000)
            except:
                return None
        else:
            try:
                return int(amount_str)
            except:
                return None

    def generate_crash_multiplier(self):
        """Генерирует множитель для краш игры с реалистичными шансами"""
        rand = random.random()

        # Большинство крашей происходят рано
        if rand < 0.4:  # 40% - краш между 1.01 и 1.50
            return round(random.uniform(1.01, 1.50), 2)
        elif rand < 0.7:  # 30% - краш между 1.50 и 3.00
            return round(random.uniform(1.50, 3.00), 2)
        elif rand < 0.85:  # 15% - краш между 3.00 и 10.00
            return round(random.uniform(3.00, 10.00), 2)
        elif rand < 0.95:  # 10% - краш между 10.00 и 50.00
            return round(random.uniform(10.00, 50.00), 2)
        elif rand < 0.99:  # 4% - краш между 50.00 и 200.00
            return round(random.uniform(50.00, 200.00), 2)
        else:  # 1% - очень редкие большие множители
            return round(random.uniform(200.00, 1500.00), 2)

    def command_crash(self, peer_id, sender_id, target_multiplier, bet_amount_str, chat_id=None):
        global CRASH_TIMERS

        # Получаем баланс пользователя
        balance_data = self.db.get_user_balance(sender_id)
        user_balance = balance_data['balance']

        # Парсим сумму ставки
        bet_amount = self.parse_amount(bet_amount_str, user_balance)

        if bet_amount is None:
            self.send_message(peer_id, '❌ Неверная сумма ставки!')
            return

        if bet_amount < 1:
            self.send_message(peer_id, '❌ Минимальная ставка: 1$!')
            return

        if bet_amount > user_balance:
            self.send_message(peer_id, f'❌ Недостаточно средств! Ваш баланс: {user_balance:,}$')
            return

        try:
            target_multiplier = float(target_multiplier)
        except ValueError:
            self.send_message(peer_id, '❌ Неверный множитель!')
            return

        if target_multiplier < 1.01:
            self.send_message(peer_id, '❌ Минимальный множитель: 1.01!')
            return

        if target_multiplier > 1000:
            self.send_message(peer_id, '❌ Максимальный множитель: 1000!')
            return

        # Списываем ставку
        self.db.update_user_balance(sender_id, -bet_amount)

        # Получаем или создаем активную игру
        active_game = self.db.get_active_crash_game(chat_id or peer_id)
        if not active_game:
            game_id = self.db.create_crash_game(chat_id or peer_id)
        else:
            game_id = active_game['id']

        # Получаем информацию о пользователе
        sender_info = self.get_user_info(sender_id)
        sender_name = sender_info['screen_name'] if sender_info else str(sender_id)

        # Добавляем ставку в базу
        self.db.add_crash_bet(game_id, sender_id, sender_name, bet_amount, target_multiplier)

        # Форматируем сумму для отображения
        if bet_amount >= 1000000:
            amount_display = f"{bet_amount/1000000:.3f}M$"
        elif bet_amount >= 1000:
            amount_display = f"{bet_amount/1000:.3f}к$"
        else:
            amount_display = f"{bet_amount}$"

        # Отправляем подтверждение ставки
        bet_confirmation = f"✅ [id{sender_id}|{sender_name}] — {amount_display} на x{target_multiplier:.2f}"
        self.send_message(peer_id, bet_confirmation)

        # Сбрасываем предыдущий таймер если он был
        timer_key = str(chat_id or peer_id)
        if timer_key in CRASH_TIMERS:
            CRASH_TIMERS[timer_key].cancel()

        # Запускаем новый таймер на 10 секунд
        timer = threading.Timer(10.0, self.end_crash_round, args=[peer_id, game_id, chat_id or peer_id])
        timer.start()
        CRASH_TIMERS[timer_key] = timer

    def end_crash_round(self, peer_id, game_id, chat_id):
        global CRASH_TIMERS

        # Удаляем таймер из словаря
        timer_key = str(chat_id)
        if timer_key in CRASH_TIMERS:
            del CRASH_TIMERS[timer_key]

        # Получаем все ставки перед закрытием
        bets = self.db.get_crash_game_bets(game_id)

        if not bets:
            self.send_message(peer_id, '📈 Игра "Crash" отменена - нет ставок.')
            self.db.end_crash_game(game_id, 0)
            return

        # Отправляем сообщение о закрытии ставок
        self.send_message(peer_id, '✅ Приём ставок для игры "Crash" закрыт.\n🕒 Итоги раунда через 5 секунд...')

        # Отправляем дополнительное сообщение о том, что ставки закрыты
        self.send_message(peer_id, '🚫 Ставки закрыты. Ожидайте результатов...')

        # Ждем 5 секунд
        time.sleep(5)

        # Генерируем результат краша
        crash_multiplier = self.generate_crash_multiplier()

        # Завершаем игру
        self.db.end_crash_game(game_id, crash_multiplier)

        # Определяем победителей и проигравших
        winners = []
        total_lost = 0

        for bet in bets:
            if bet['target_multiplier'] <= crash_multiplier:
                # Игрок выиграл
                win_amount = int(bet['bet_amount'] * bet['target_multiplier'])
                self.db.update_user_balance(bet['user_id'], win_amount)
                winners.append({
                    'user_id': bet['user_id'],
                    'username': bet['username'],
                    'bet_amount': bet['bet_amount'],
                    'win_amount': win_amount,
                    'target_multiplier': bet['target_multiplier']
                })
            else:
                # Игрок проиграл
                total_lost += bet['bet_amount']

        # Формируем итоговое сообщение
        result_text = f"📈 Итоги игры \"Crash\"\n📈 Краш на отметке: x{crash_multiplier:.2f}\n\n"

        if winners:
            for winner in winners:
                # Форматируем суммы для отображения
                if winner['bet_amount'] >= 1000000:
                    bet_display = f"{winner['bet_amount']/1000000:.3f}M$"
                elif winner['bet_amount'] >= 1000:
                    bet_display = f"{winner['bet_amount']/1000:.3f}к$"
                else:
                    bet_display = f"{winner['bet_amount']}$"

                if winner['win_amount'] >= 1000000:
                    win_display = f"{winner['win_amount']/1000000:.3f}M$"
                elif winner['win_amount'] >= 1000:
                    win_display = f"{winner['win_amount']/1000:.3f}к$"
                else:
                    win_display = f"{winner['win_amount']}$"

                result_text += f"✅ [id{winner['user_id']}|{winner['username']}] — {bet_display} на {winner['target_multiplier']:.2f}\n— Приз: {win_display}\n\n"

        if total_lost > 0:
            if winners:
                result_text += "\n"
            else:
                result_text += "❌ Все ставки проиграли!\n\n"

            # Форматируем проигранную сумму
            if total_lost >= 1000000:
                lost_display = f"{total_lost/1000000:.3f}M$"
            elif total_lost >= 1000:
                lost_display = f"{total_lost/1000:.3f}к$"
            else:
                lost_display = f"{total_lost}$"

            result_text += f"💰 Проиграно: {lost_display}"

        self.send_message(peer_id, result_text)

    def command_dream(self, peer_id, sender_id, target_multiplier, bet_amount_str, chat_id=None):
        """Команда Дрим - аналог краша с более высокими множителями"""
        global CRASH_TIMERS

        # Получаем баланс пользователя
        balance_data = self.db.get_user_balance(sender_id)
        user_balance = balance_data['balance']

        # Парсим сумму ставки
        bet_amount = self.parse_amount(bet_amount_str, user_balance)

        if bet_amount is None:
            self.send_message(peer_id, '❌ Неверная сумма ставки!')
            return

        if bet_amount < 1:
            self.send_message(peer_id, '❌ Минимальная ставка: 1$!')
            return

        if bet_amount > user_balance:
            self.send_message(peer_id, f'❌ Недостаточно средств! Ваш баланс: {user_balance:,}$')
            return

        try:
            target_multiplier = float(target_multiplier)
        except ValueError:
            self.send_message(peer_id, '❌ Неверный множитель!')
            return

        if target_multiplier < 1.01:
            self.send_message(peer_id, '❌ Минимальный множитель: 1.01!')
            return

        if target_multiplier > 10000:  # Дрим может иметь более высокие множители
            self.send_message(peer_id, '❌ Максимальный множитель: 10000!')
            return

        # Списываем ставку
        self.db.update_user_balance(sender_id, -bet_amount)

        # Получаем или создаем активную игру (используем ту же таблицу что и краш)
        active_game = self.db.get_active_crash_game(chat_id or peer_id)
        if not active_game:
            game_id = self.db.create_crash_game(chat_id or peer_id)
        else:
            game_id = active_game['id']

        # Получаем информацию о пользователе
        sender_info = self.get_user_info(sender_id)
        sender_name = sender_info['screen_name'] if sender_info else str(sender_id)

        # Добавляем ставку в базу (используем ту же таблицу что и краш)
        self.db.add_crash_bet(game_id, sender_id, sender_name, bet_amount, target_multiplier)

        # Форматируем сумму для отображения
        if bet_amount >= 1000000:
            amount_display = f"{bet_amount/1000000:.3f}M$"
        elif bet_amount >= 1000:
            amount_display = f"{bet_amount/1000:.3f}к$"
        else:
            amount_display = f"{bet_amount}$"

        # Отправляем подтверждение ставки
        bet_confirmation = f"💭 [id{sender_id}|{sender_name}] — {amount_display} на x{target_multiplier:.2f} (Дрим)"
        self.send_message(peer_id, bet_confirmation)

        # Сбрасываем предыдущий таймер если он был
        timer_key = str(chat_id or peer_id)
        if timer_key in CRASH_TIMERS:
            CRASH_TIMERS[timer_key].cancel()

        # Запускаем новый таймер на 15 секунд (дольше чем краш)
        timer = threading.Timer(15.0, self.end_dream_round, args=[peer_id, game_id, chat_id or peer_id])
        timer.start()
        CRASH_TIMERS[timer_key] = timer

    def end_dream_round(self, peer_id, game_id, chat_id):
        """Завершение раунда Дрим"""
        global CRASH_TIMERS

        # Удаляем таймер из словаря
        timer_key = str(chat_id)
        if timer_key in CRASH_TIMERS:
            del CRASH_TIMERS[timer_key]

        # Получаем все ставки перед закрытием
        bets = self.db.get_crash_game_bets(game_id)

        if not bets:
            self.send_message(peer_id, '💭 Игра "Дрим" отменена - нет ставок.')
            self.db.end_crash_game(game_id, 0)
            return

        # Отправляем сообщение о закрытии ставок
        self.send_message(peer_id, '✅ Приём ставок для игры "Дрим" закрыт.\n🕒 Итоги раунда через 7 секунд...')

        # Отправляем дополнительное сообщение о том, что ставки закрыты
        self.send_message(peer_id, '🚫 Ставки закрыты. Ожидайте результатов...')

        # Ждем 7 секунд
        time.sleep(7)

        # Генерируем результат дрима (более высокие множители)
        dream_multiplier = self.generate_dream_multiplier()

        # Завершаем игру
        self.db.end_crash_game(game_id, dream_multiplier)

        # Определяем победителей и проигравших
        winners = []
        total_lost = 0

        for bet in bets:
            if bet['target_multiplier'] <= dream_multiplier:
                # Игрок выиграл
                win_amount = int(bet['bet_amount'] * bet['target_multiplier'])
                self.db.update_user_balance(bet['user_id'], win_amount)
                winners.append({
                    'user_id': bet['user_id'],
                    'username': bet['username'],
                    'bet_amount': bet['bet_amount'],
                    'win_amount': win_amount,
                    'target_multiplier': bet['target_multiplier']
                })
            else:
                # Игрок проиграл
                total_lost += bet['bet_amount']

        # Формируем итоговое сообщение
        result_text = f"💭 Итоги игры \"Дрим\"\n💭 Дрим на отметке: x{dream_multiplier:.2f}\n\n"

        if winners:
            for winner in winners:
                # Форматируем суммы для отображения
                if winner['bet_amount'] >= 1000000:
                    bet_display = f"{winner['bet_amount']/1000000:.3f}M$"
                elif winner['bet_amount'] >= 1000:
                    bet_display = f"{winner['bet_amount']/1000:.3f}к$"
                else:
                    bet_display = f"{winner['bet_amount']}$"

                if winner['win_amount'] >= 1000000:
                    win_display = f"{winner['win_amount']/1000000:.3f}M$"
                elif winner['win_amount'] >= 1000:
                    win_display = f"{winner['win_amount']/1000:.3f}к$"
                else:
                    win_display = f"{winner['win_amount']}$"

                result_text += f"✅ [id{winner['user_id']}|{winner['username']}] — {bet_display} на {winner['target_multiplier']:.2f}\n— Приз: {win_display}\n\n"

        if total_lost > 0:
            if winners:
                result_text += "\n"
            else:
                result_text += "❌ Все ставки проиграли!\n\n"

            # Форматируем проигранную сумму
            if total_lost >= 1000000:
                lost_display = f"{total_lost/1000000:.3f}M$"
            elif total_lost >= 1000:
                lost_display = f"{total_lost/1000:.3f}к$"
            else:
                lost_display = f"{total_lost}$"

            result_text += f"💰 Проиграно: {lost_display}"

        self.send_message(peer_id, result_text)

    def generate_dream_multiplier(self):
        """Генерирует множитель для игры Дрим (более высокие значения)"""
        # Дрим имеет более высокие множители чем краш
        # 50% шанс на множитель до 2x
        # 30% шанс на множитель до 5x  
        # 15% шанс на множитель до 10x
        # 4% шанс на множитель до 50x
        # 1% шанс на множитель до 100x

        rand = random.random()

        if rand < 0.5:
            return random.uniform(1.01, 2.0)
        elif rand < 0.8:
            return random.uniform(2.0, 5.0)
        elif rand < 0.95:
            return random.uniform(5.0, 10.0)
        elif rand < 0.99:
            return random.uniform(10.0, 50.0)
        else:
            return random.uniform(50.0, 100.0)

    def command_add_balance(self, peer_id, sender_id, target_id, amount):
        if not target_id or not amount:
            self.send_message(peer_id, '❌ Использование: !add [@пользователь] [сумма] или !add [сумма] (для себя)')
            return

        try:
            amount = int(amount)
        except ValueError:
            self.send_message(peer_id, '❌ Сумма должна быть числом!')
            return

        sender_info = self.get_user_info(sender_id)
        sender_name = sender_info['screen_name'] if sender_info else str(sender_id)

        # Проверяем, является ли отправитель гранд-менеджером
        if sender_name != CONFIG['grand_manager']:
            self.send_message(peer_id, '❌ Только создатель может использовать эту команду!')
            return

        try:
            self.db.set_user_balance(target_id, amount)

            target_info = self.get_user_info(target_id)
            target_name = target_info['screen_name'] if target_info else str(target_id)

            self.send_message(peer_id, f'✅ Баланс @{target_name} установлен: {amount:,} монет')
            self.log(f"Баланс пользователя {target_name} установлен на {amount:,} создателем {sender_name}")

        except Exception as e:
            self.log(f"Ошибка установки баланса: {e}")
            self.send_message(peer_id, '❌ Ошибка при установке баланса.')

    def command_set_support_chat(self, peer_id, sender_id, chat_id):
        """Установить чат для получения репортов"""
        system_admin = self.db.get_system_admin(sender_id)
        if not system_admin or system_admin['access_level'] < 4:
            self.send_message(peer_id, '❌ Только основатель бота может устанавливать чат поддержки.')
            return

        try:
            self.db.set_support_chat(peer_id)
            self.send_message(peer_id, f'✅ Этот чат установлен для получения репортов от пользователей.')
            self.log(f"Чат поддержки установлен: {peer_id}")
        except Exception as e:
            self.log(f"Ошибка установки чата поддержки: {e}")
            self.send_message(peer_id, '❌ Ошибка при установке чата поддержки.')

    def check_command_permission(self, command, user_id, username, chat_id=None):
        """Проверяет, имеет ли пользователь право выполнять команду."""
        required_level = 0

        # Системные команды (проверяются отдельно)
        if command in ['ahelp', 'sysadmins', 'tickets', 'giveagent', 'giveadm', 'null', 'sysban', 'unsysban', 'sysrole']:
            return {'has_permission': True}  # Проверка внутри самих команд

        # Проверяем кастомные права для команды в конкретном чате
        if chat_id:
            try:
                cursor = self.db.conn.cursor()
                cursor.execute(
                    'SELECT required_level FROM command_permissions WHERE chat_id = ? AND command = ?',
                    (chat_id, command)
                )
                custom_perm = cursor.fetchone()
                if custom_perm:
                    required_level = custom_perm['required_level']
                    has_permission = self.has_permission(user_id, username, required_level, chat_id)

                    if not has_permission and required_level > 0:
                        user_role = self.get_user_role(user_id, chat_id)
                        required_role_name = self.get_role_name_for_level(required_level, chat_id)
                        return {
                            'has_permission': False,
                            'required_level': required_level,
                            'required_role_name': required_role_name,
                            'user_role_name': user_role['name'],
                            'user_level': user_role['level'],
                            'command': command
                        }
                    return {'has_permission': has_permission}
            except Exception as e:
                self.log(f"Ошибка проверки кастомных прав команды: {e}")

        # Определяем минимальный уровень прав для команды (стандартные уровни)
        if command in ['helper']:
            required_level = 20  # Помощник
        elif command in ['kick', 'warn', 'unwarn', 'getwarn', 'warnhistory', 'warnlist', 'mutelist', 'mute', 'unmute', 'getban', 'setnick', 'removenick', 'getbynick', 'nicknames', 'nonames', 'zov', 'roles']:
            required_level = 20  # Помощник - базовые команды модерации
        elif command in ['role', 'removerole', 'ban', 'unban', 'banlist', 'gkick', 'silence', 'logs', 'gsetnick', 'gremovenick', 'delete', 'gzov', 'newrole', 'delrole']:
            required_level = 40  # Модератор - расширенные команды администрирования
        elif command in ['admin', 'moder', 'gm', 'gms', 'gban', 'gunban', 'filter', 'settings', 'pin', 'unpin', 'rr', 'gsetrole', 'welcome']:
            required_level = 60  # Администратор - глобальное управление и настройки
        elif command in ['gdelrole', 'setrules', 'inactive']:
            required_level = 80  # Спец.Администратор
        elif command in ['owner', 'initadmin', 'checknicks', 'editcmd', 'pull', 'newpull', 'wipe', 'piar']:
            required_level = 100 # Создатель
        elif command in ['add']: # Команда add доступна только создателю
            required_level = 100
        elif command in ['help', 'ping', 'start', 'rules', 'try', 'kiss', 'hug', 'marry', 'divorce', 'roulette', 'bet', 'bonus', 'balance', 'report', 'stats', 'online', 'staff', 'top', 'getreport', 'chatinfo', 'getnick', 'nicknames', 'nonames', 'zov', 'reg', 'settoken', 'answer', 'rape', 'oral', 'mtop', 'crash', 'dream', 'dice', 'convert', 'transfer', 'q', 'chatid', 'ai']:
            required_level = 0 # Любой пользователь

        has_permission = self.has_permission(user_id, username, required_level, chat_id)

        if not has_permission and required_level > 0:
            # Получаем текущую роль пользователя
            user_role = self.get_user_role(user_id, chat_id)
            required_role_name = self.get_role_name_for_level(required_level, chat_id)

            return {
                'has_permission': False,
                'required_level': required_level,
                'required_role_name': required_role_name,
                'user_role_name': user_role['name'],
                'user_level': user_role['level'],
                'command': command
            }

        return {'has_permission': has_permission}

    def handle_command(self, text, user_id, username, peer_id, chat_id, message):
        # Проверяем системный бан
        if self.db.is_system_banned(user_id):
            ban_info = self.db.get_system_ban(user_id)
            if ban_info:
                ban_text = "🚫 ВЫ ЗАБЛОКИРОВАНЫ В СИСТЕМЕ БОТА\n\n"
                ban_text += f"📝 Причина: {ban_info['reason']}\n"
                ban_text += f"📅 Дата бана: {ban_info['created_at'][:10]}\n"
                if ban_info['banned_until']:
                    ban_text += f"⏰ До: {ban_info['banned_until'][:10]}\n"
                else:
                    ban_text += "⏰ Срок: Навсегда\n"
                ban_text += "\n❌ Вы не можете использовать команды бота"
                self.send_message(peer_id, ban_text)
                return

        # Убираем префикс команды
        command_text = text[1:].strip()

        if not command_text:
            return

        # Разбиваем на аргументы
        args = command_text.split()
        command = args[0].lower()

        # Специальная обработка команды add для создателя
        if command == 'add' and self.has_permission(user_id, username, 100, chat_id):
            if len(args) >= 2:
                try:
                    amount = int(args[1])
                    self.db.set_user_balance(user_id, amount)
                    self.send_message(peer_id, f'💰 Баланс установлен: {amount:,} $')
                except ValueError:
                    self.send_message(peer_id, '❌ Неверная сумма')
            return

        # Специальная обработка команды установить_чат для основателя
        if command == 'установить_чат':
            self.command_set_support_chat(peer_id, user_id, chat_id)
            return

        # Определяем команду по алиасам
        original_command = command
        for cmd_key, aliases in CONFIG['commands'].items():
            if command in aliases:
                command = cmd_key
                break

        # Проверка регистрации беседы (кроме команды start)
        if chat_id and command not in ['start', 'начать', 'старт'] and not self.is_chat_registered(chat_id):
            error_message = """Беседа не активирована! Нажмите «Активировать» и продолжайте пользоваться ботом в вашей беседе!"""

            # Добавляем inline-кнопку активации
            keyboard = {
                "inline": True,
                "buttons": [
                    [{
                        "action": {
                            "type": "callback",
                            "label": "Активировать",
                            "payload": json.dumps({"action": "activate_chat"})
                        },
                        "color": "primary"
                    }]
                ]
            }

            self.send_message(peer_id, error_message, json.dumps(keyboard))
            return

        # Проверка прав доступа к командам
        permission_check = self.check_command_permission(command, user_id, username, chat_id)
        if not permission_check['has_permission']:
            if 'required_level' in permission_check:
                error_message = f"""⛔ Доступ запрещён! Для команды /{permission_check['command']} нужен приоритет ({permission_check['required_level']}) и выше.

👤 У вас: {permission_check['user_role_name']} ({permission_check['user_level']})"""
                self.send_message(peer_id, error_message)
            else:
                self.send_message(peer_id, '❌ У вас недостаточно прав для выполнения этой команды.')
            return

        # Далее идет обработка команд, как было раньше
        if command == 'help':
            self.command_help(peer_id)

        elif command == 'ping':
            self.command_ping(peer_id)

        elif command == 'start':
            self.command_start(peer_id, user_id, chat_id)

        elif command == 'rules':
            self.command_rules(peer_id)

        elif command == 'roles':
            self.command_roles(peer_id, chat_id)

        elif command == 'try':
            action = ' '.join(args[1:])
            self.command_try(peer_id, action)

        elif command == 'kiss':
            target_id = self.get_target_user_from_command(message, args)
            self.command_kiss(peer_id, user_id, target_id)

        elif command == 'hug':
            target_id = self.get_target_user_from_command(message, args)
            self.command_hug(peer_id, user_id, target_id)

        elif command == 'marry':
            target_id = self.get_target_user_from_command(message, args)
            self.command_marry(peer_id, user_id, target_id)

        elif command == 'divorce':
            self.command_divorce(peer_id, user_id)

        elif command == 'rape':
            target_id = self.get_target_user_from_command(message, args)
            self.command_rape(peer_id, user_id, target_id)

        elif command == 'oral':
            target_id = self.get_target_user_from_command(message, args)
            self.command_oral(peer_id, user_id, target_id)

        elif command == 'roulette':
            self.command_roulette(peer_id, user_id)

        elif command == 'bet':
            if len(args) < 3:
                self.send_message(peer_id, '❌ Использование: /ставка [тип] [сумма] или /ставка [число] [сумма]')
                return

            bet_type = args[1]

            # Ставка на конкретное число
            try:
                number = int(bet_type)
                if 0 <= number <= 36:
                    bet_amount = args[2]
                    self.command_bet(peer_id, user_id, 'число', bet_amount, bet_type, chat_id)
                else:
                    self.send_message(peer_id, '❌ Номер должен быть от 0 до 36!')
            except ValueError:
                # Ставка на тип (чет/нечет/красное/черное)
                if bet_type in ['чет', 'нечет', 'красное', 'черное']:
                    bet_amount = args[2]
                    self.command_bet(peer_id, user_id, bet_type, bet_amount, None, chat_id)
                else:
                    self.send_message(peer_id, '❌ Доступные ставки: чет, нечет, красное, черное или число от 0 до 36')

        elif command == 'bonus':
            self.command_bonus(peer_id, user_id)

        elif command == 'crash':
            if len(args) < 3:
                self.send_message(peer_id, '❌ Использование: /краш [множитель] [сумма]')
                return

            target_multiplier = args[1]
            bet_amount_str = args[2]
            self.command_crash(peer_id, user_id, target_multiplier, bet_amount_str, chat_id)

        elif command == 'dream':
            if len(args) < 3:
                self.send_message(peer_id, '❌ Использование: /дрим [множитель] [сумма]')
                return

            target_multiplier = args[1]
            bet_amount_str = args[2]
            self.command_dream(peer_id, user_id, target_multiplier, bet_amount_str, chat_id)

        elif command == 'add': # Обработка команды add здесь, если она не была обработана выше для создателя
            if len(args) < 2:
                self.send_message(peer_id, '❌ Использование: !add [сумма] или !add [@ID] [сумма]')
                return

            if len(args) == 2:
                # Установить баланс себе
                amount = args[1]
                self.command_add_balance(peer_id, user_id, user_id, amount)
            else:
                # Установить баланс другому пользователю
                target_id = self.resolve_user_id(args[1])
                amount = args[2]
                if target_id:
                    self.command_add_balance(peer_id, user_id, target_id, amount)
                else:
                    self.send_message(peer_id, '❌ Пользователь не найден!')

        elif command == 'balance':
            self.command_balance(peer_id, user_id)

        elif command == 'report':
            text = ' '.join(args[1:])
            self.command_report(peer_id, user_id, text)

        # Модерационные команды
        elif command == 'warn':
            target_id = self.get_target_user_from_command(message, args)

            # Определяем причину в зависимости от способа вызова команды
            if message.get('reply_message'):
                # Если это ответ на сообщение: /warn п1.2
                reason = ' '.join(args[1:]) if len(args) > 1 else ''
            else:
                # Если это упоминание: /warn @user п1.2
                reason = ' '.join(args[2:]) if len(args) > 2 else ''

            self.command_warn(peer_id, user_id, target_id, reason, chat_id)

        elif command == 'kick':
            target_id = self.get_target_user_from_command(message, args)

            # Проверяем, указан ли пользователь
            if not target_id:
                error_message = """☕️ Аргументы введены неверно. Вы не указали ппользователя для исключения.

☕️ Примеры использования:
/kick @user причина
/kick @user
/kick - ответом на сообщение"""
                self.send_message(peer_id, error_message)
                return

            reason = ' '.join(args[2:]) if len(args) > 2 else 'Нарушение правил'
            if message.get('reply_message'):
                reason = ' '.join(args[1:]) if len(args) > 1 else 'Нарушение правил'

            self.command_kick(peer_id, user_id, target_id, reason, chat_id)

        elif command == 'ban':
            target_id = self.get_target_user_from_command(message, args)

            # Парсим дни и причину
            days = None
            reason = 'Серьезное нарушение правил'

            if message.get('reply_message'):
                # Если ответ на сообщение: /ban [дни] [причина]
                if len(args) > 1 and args[1].isdigit():
                    days = int(args[1])
                    reason = ' '.join(args[2:]) if len(args) > 2 else 'Не указана'
                elif len(args) > 1:
                    reason = ' '.join(args[1:])
            else:
                # Если упоминание: /ban @user [дни] [причина]
                if len(args) > 2 and args[2].isdigit():
                    days = int(args[2])
                    reason = ' '.join(args[3:]) if len(args) > 3 else 'Не указана'
                elif len(args) > 2:
                    reason = ' '.join(args[2:])

            self.command_ban(peer_id, user_id, target_id, reason, chat_id, days)

        elif command == 'mute':
            target_id = self.get_target_user_from_command(message, args)
            duration = 60  # по умолчанию 60 минут
            reason = 'Спам или флуд'

            if message.get('reply_message'):
                if len(args) > 1 and args[1].isdigit():
                    duration = int(args[1])
                if len(args) > 2:
                    reason = ' '.join(args[2:])
            else:
                if len(args) > 2 and args[2].isdigit():
                    duration = int(args[2])
                if len(args) > 3:
                    reason = ' '.join(args[3:])

            self.command_mute(peer_id, user_id, target_id, duration, reason, chat_id)

        elif command == 'unmute':
            target_id = self.get_target_user_from_command(message, args)
            self.command_unmute(peer_id, user_id, target_id, chat_id)

        elif command == 'unban':
            target_id = self.get_target_user_from_command(message, args)
            self.command_unban(peer_id, user_id, target_id, chat_id)

        elif command == 'newrole':
            if len(args) < 3:
                self.send_message(peer_id, '⛔️ Отказано! /newrole [приоретет] [название]')
                return

            role_level = args[1]
            role_name = ' '.join(args[2:])

            self.command_newrole(peer_id, user_id, None, role_level, role_name, chat_id)

        elif command == 'stats':
            target_id = self.get_target_user_from_command(message, args)
            self.command_stats(peer_id, user_id, target_id, chat_id)

        elif command == 'online':
            self.command_online(peer_id, chat_id)

        elif command == 'staff':
            self.command_staff(peer_id, chat_id)

        elif command == 'unwarn':
            target_id = self.get_target_user_from_command(message, args)
            self.command_unwarn(peer_id, user_id, target_id, chat_id)

        elif command == 'getwarn':
            target_id = self.get_target_user_from_command(message, args)
            self.command_getwarn(peer_id, target_id)

        elif command == 'getreport':
            self.command_getreport(peer_id, user_id)

        elif command == 'helper':
            target_id = self.get_target_user_from_command(message, args)
            self.command_helper(peer_id, user_id, target_id, chat_id)

        elif command == 'gm':
            target_id = self.get_target_user_from_command(message, args)
            self.command_gm(peer_id, user_id, target_id, chat_id)

        elif command == 'gms':
            self.command_gms(peer_id, chat_id)

        elif command == 'grm':
            target_id = self.get_target_user_from_command(message, args)
            self.command_grm(peer_id, user_id, target_id, chat_id)

        elif command == 'banlist':
            self.command_banlist(peer_id, chat_id)

        elif command == 'top':
            self.command_top(peer_id)

        elif command == 'mtop':
            self.command_mtop(peer_id)

        elif command == 'sysadmins':
            self.command_sysadmins(peer_id, user_id)

        elif command == 'broadcast':
            text = ' '.join(args[1:])
            self.command_broadcast(peer_id, user_id, text)

        elif command == 'answer':
            if len(args) < 3:
                self.send_message(peer_id, '❌ Использование: /answer [ID] [ответ]')
                return

            ticket_id = args[1]
            answer = ' '.join(args[2:])
            self.command_answer(peer_id, user_id, ticket_id, answer)

        elif command == 'settoken':
            self.command_settoken(peer_id)

        elif command == 'silence':
            self.command_silence(peer_id, user_id, chat_id)

        elif command == 'getbynick':
            nickname = ' '.join(args[1:])
            self.command_getbynick(peer_id, nickname)

        elif command == 'warnhistory':
            target_id = self.get_target_user_from_command(message, args)
            self.command_warnhistory(peer_id, target_id)

        elif command == 'warnlist':
            self.command_warnlist(peer_id, chat_id)

        elif command == 'mutelist':
            self.command_mutelist(peer_id, chat_id)

        elif command == 'getban':
            target_id = self.get_target_user_from_command(message, args)
            self.command_getban(peer_id, target_id, chat_id)

        elif command == 'getnick':
            target_id = self.get_target_user_from_command(message, args)
            self.command_getnick(peer_id, target_id, chat_id)

        elif command == 'setnick':
            target_id = self.get_target_user_from_command(message, args)

            # Определяем никнейм в зависимости от способа вызова команды
            if message.get('reply_message'):
                # Если это ответ на сообщение: /snick Никнейм
                nickname = ' '.join(args[1:]) if len(args) > 1 else ''
            else:
                # Если это упоминание: /snick @user Никнейм
                nickname = ' '.join(args[2:]) if len(args) > 2 else ''

            self.command_setnick(peer_id, user_id, target_id, nickname, chat_id)

        elif command == 'removenick':
            target_id = self.get_target_user_from_command(message, args)
            self.command_removenick(peer_id, user_id, target_id, chat_id)

        elif command == 'nicknames':
            self.command_nicknames(peer_id, chat_id)

        elif command == 'nonames':
            self.command_nonames(peer_id, chat_id)

        elif command == 'zov':
            text = ' '.join(args[1:])
            self.command_zov(peer_id, user_id, text, chat_id)

        elif command == 'reg':
            target_id = self.get_target_user_from_command(message, args)
            self.command_reg(peer_id, target_id)

        elif command == 'checknicks':
            self.command_checknicks(peer_id, user_id)

        elif command == 'chatinfo':
            self.command_chatinfo(peer_id, chat_id)

        elif command == 'moder':
            target_id = self.get_target_user_from_command(message, args)
            self.command_moder(peer_id, user_id, target_id, chat_id)

        elif command == 'admin':
            target_id = self.get_target_user_from_command(message, args)
            self.command_admin(peer_id, user_id, target_id, chat_id)

        elif command == 'owner':
            target_id = self.get_target_user_from_command(message, args)
            self.command_owner(peer_id, user_id, target_id, chat_id)

        elif command == 'removerole':
            target_id = self.get_target_user_from_command(message, args)
            self.command_removerole(peer_id, user_id, target_id, chat_id)

        elif command == 'delete':
            self.command_delete(peer_id, user_id, message, chat_id)

        elif command == 'gkick':
            target_id = self.get_target_user_from_command(message, args)
            reason = ' '.join(args[2:]) if len(args) > 2 else 'Нарушение правил'
            self.command_gkick(peer_id, user_id, target_id, reason, chat_id)

        elif command == 'logs':
            page = int(args[1]) if len(args) > 1 else 1
            self.command_logs(peer_id, chat_id, page)

        elif command == 'gsetnick':
            target_id = self.get_target_user_from_command(message, args)
            nickname = ' '.join(args[2:]) if len(args) > 2 else None
            self.command_gsetnick(peer_id, user_id, target_id, nickname, chat_id)

        elif command == 'gremovenick':
            target_id = self.get_target_user_from_command(message, args)
            self.command_gremovenick(peer_id, user_id, target_id, chat_id)

        elif command == 'gzov':
            message_text = ' '.join(args[1:]) if len(args) > 1 else ''
            self.command_gzov(peer_id, user_id, message_text, chat_id)

        elif command == 'gban':
            target_id = self.get_target_user_from_command(message, args)
            reason = ' '.join(args[2:]) if len(args) > 2 else 'Нарушение правил'
            self.command_gban(peer_id, user_id, target_id, reason, chat_id)

        elif command == 'gunban':
            target_id = self.get_target_user_from_command(message, args)
            self.command_gunban(peer_id, user_id, target_id, chat_id)

        elif command == 'filter':
            self.command_filter(peer_id, user_id, args, chat_id)

        elif command == 'settings':
            self.command_settings(peer_id, user_id, chat_id)

        elif command == 'rr':
            target_id = self.get_target_user_from_command(message, args)
            self.command_rr(peer_id, user_id, target_id, chat_id)

        elif command == 'gnewrole':
            if len(args) < 3:
                self.send_message(peer_id, '⛔️ Отказано! Использование: /gnewrole [приоритет] [название]')
                return

            role_level = args[1]
            role_name = ' '.join(args[2:])
            self.command_gnewrole(peer_id, user_id, role_level, role_name, chat_id)

        elif command == 'gsetrole':
            target_id = self.get_target_user_from_command(message, args)
            if message.get('reply_message'):
                role_level = args[1] if len(args) >= 2 else None
            else:
                role_level = args[2] if len(args) >= 3 else None
            self.command_gsetrole(peer_id, user_id, target_id, role_level, chat_id)

        elif command == 'pin':
            self.command_pin(peer_id, user_id, message, chat_id)

        elif command == 'unpin':
            self.command_unpin(peer_id, user_id, chat_id)

        elif command == 'role':
            target_id = self.get_target_user_from_command(message, args)

            # Определяем где находится уровень/название роли
            if message.get('reply_message'):
                # Если ответ на сообщение: /role 10 или /role Администратор
                role_level = args[1] if len(args) >= 2 else None
            else:
                # Если через упоминание: /role @user 10 или /role @user Администратор
                role_level = args[2] if len(args) >= 3 else None

            self.command_role(peer_id, user_id, target_id, role_level, chat_id)

        elif command == 'delrole':
            if len(args) < 2:
                self.send_message(peer_id, 'Отказано! Необходимо указать роль /delrole [уровень]')
                return

            role_level = args[1]
            self.command_delrole(peer_id, user_id, role_level, chat_id)

        elif command == 'gdelrole':
            if len(args) < 2:
                self.send_message(peer_id, '❌ Использование: /gdelrole [уровень]')
                return

            role_level = args[1]
            self.command_gdelrole(peer_id, user_id, role_level)

        elif command == 'welcome':
            text = ' '.join(args[1:])
            self.command_welcome(peer_id, user_id, text, chat_id)

        elif command == 'setrules':
            text = ' '.join(args[1:])
            self.command_setrules(peer_id, user_id, text, chat_id)

        elif command == 'inactive':
            if len(args) < 2:
                self.send_message(peer_id, '❌ Использование: /inactive [дни]')
                return

            days = args[1]
            self.command_inactive(peer_id, user_id, days, chat_id)

        elif command == 'initadmin':
            self.command_initadmin(peer_id, user_id)

        # Команда convert
        elif command == 'convert':
            if len(args) < 2:
                self.send_message(peer_id, '❌ Использование: /перевед [число]')
                return

            number = args[1]
            converted = self.convert_number_to_short(number)
            if converted:
                self.send_message(peer_id, f'🔢 {number} → {converted}')
            else:
                self.send_message(peer_id, '❌ Неверное число')

        # Команда transfer
        elif command == 'transfer':
            if len(args) < 2:
                self.send_message(peer_id, '❌ Использование: /перевод [сумма] [ID] или ответьте на сообщение')
                return

            # Получаем сумму
            balance_data = self.db.get_user_balance(user_id)
            user_balance = balance_data['balance']

            transfer_amount = self.parse_amount(args[1], user_balance)
            if transfer_amount is None or transfer_amount <= 0:
                self.send_message(peer_id, '❌ Неверная сумма для перевода!')
                return

            # Получаем получателя
            target_id = self.get_target_user_from_command(message, args, 2)
            if not target_id:
                self.send_message(peer_id, 'Отказано! Укажите получателя: ответьте на сообщение или укажите пользователя')
                return

            if target_id == user_id:
                self.send_message(peer_id, '❌ Нельзя переводить деньги самому себе!')
                return

            # Выполняем перевод
            success, message_text = self.db.transfer_balance(user_id, target_id, transfer_amount)

            if success:
                sender_info = self.get_user_info(user_id)
                target_info = self.get_user_info(target_id)
                sender_name = sender_info['screen_name'] if sender_info else str(user_id)
                target_name = target_info['screen_name'] if target_info else str(target_id)

                # Форматируем сумму для отображения
                amount_display = self.convert_number_to_short(transfer_amount) or f"{transfer_amount:,}"

                transfer_text = f"""💸 ПЕРЕВОД ВЫПОЛНЕН

📤 От: @{sender_name}
📥 Кому: @{target_name}
💰 Сумма: {amount_display}$

✅ Перевод успешно завершен!"""

                self.send_message(peer_id, transfer_text)
                self.log(f"Перевод {transfer_amount}$ от {sender_name} к {target_name}")
            else:
                self.send_message(peer_id, f'❌ {message_text}')

        elif command == 'dice':
            dice_args = args[1:] if len(args) > 1 else []
            self.command_dice(peer_id, user_id, dice_args, chat_id)

        # Системные команды
        elif command == 'ahelp':
            self.command_ahelp(peer_id, user_id)

        elif command == 'sysadmins':
            self.command_sysadmins(peer_id)

        elif command == 'giveagent':
            target_id = self.get_target_user_from_command(message, args)
            self.command_giveagent(peer_id, user_id, target_id)

        elif command == 'giveadm':
            target_id = self.get_target_user_from_command(message, args)
            self.command_giveadm(peer_id, user_id, target_id)

        elif command == 'givezam':
            target_id = self.get_target_user_from_command(message, args)
            self.command_givezam(peer_id, user_id, target_id)

        elif command == 'giveowner':
            target_id = self.get_target_user_from_command(message, args)
            self.command_giveowner(peer_id, user_id, target_id)

        elif command == 'null':
            target_id = self.get_target_user_from_command(message, args)
            self.command_null(peer_id, user_id, target_id)

        elif command == 'sysban':
            target_id = self.get_target_user_from_command(message, args)
            if len(args) < 3:
                self.send_message(peer_id, '❌ Использование: /sysban [ID] [дни] [причина]')
                return

            days = args[2] if not message.get('reply_message') else args[1]
            reason = ' '.join(args[3:]) if not message.get('reply_message') else ' '.join(args[2:])

            if not reason:
                reason = "Нарушение правил системы"

            self.command_sysban(peer_id, user_id, target_id, days, reason)

        elif command == 'unsysban':
            target_id = self.get_target_user_from_command(message, args)
            self.command_unsysban(peer_id, user_id, target_id)

        elif command == 'sysrole':
            target_id = self.get_target_user_from_command(message, args)
            if len(args) < 3:
                self.send_message(peer_id, '❌ Использование: /sysrole [ID] [уровень]')
                return

            role_level = args[2] if not message.get('reply_message') else args[1]
            self.command_sysrole(peer_id, user_id, target_id, role_level, chat_id)

        elif command == 'tickets':
            self.command_tickets(peer_id, user_id)

        elif command == 'q':
            self.command_q(peer_id, user_id, chat_id)

        elif command == 'chatid':
            self.command_chatid(peer_id, chat_id)

        elif command == 'editcmd':
            if len(args) < 3:
                self.send_message(peer_id, '❌ /editcmd [команда] [приоритет]')
                return

            cmd = args[1]
            level = args[2]
            self.command_editcmd(peer_id, user_id, cmd, level, chat_id)

        elif command == 'pull':
            if len(args) < 2:
                self.send_message(peer_id, '❌ /pull [ключ объединения]')
                return

            union_key = args[1]
            self.command_pull(peer_id, user_id, union_key, chat_id)

        elif command == 'newpull':
            if len(args) < 2:
                self.send_message(peer_id, '❌ /newpull [название объединения]')
                return

            union_name = ' '.join(args[1:])
            self.command_pull(peer_id, user_id, union_name, chat_id)

        elif command == 'pullinfo':
            self.command_pullinfo(peer_id, user_id, chat_id)

        elif command == 'pulldel':
            self.command_pulldel(peer_id, user_id, chat_id)

        elif command == 'wipe':
            if len(args) < 2:
                self.send_message(peer_id, '❌ Использование: /wipe [bans|warn|nick|roles]')
                return

            wipe_type = args[1].lower()
            self.command_wipe(peer_id, user_id, wipe_type, chat_id)

        elif command == 'ai':
            question = ' '.join(args[1:])
            self.command_ai(peer_id, user_id, question)

        elif command == 'piar':
            if len(args) < 2:
                self.send_message(peer_id, '❌ Использование: /piar [текст] [минуты]\n💡 Или /piar стоп для остановки')
                return

            # Проверка на команду "стоп"
            if args[1].lower() in ['стоп', 'stop', 'остановить']:
                self.command_piar(peer_id, user_id, '', 0, chat_id)
                return

            # Ищем число в конце аргументов
            interval_minutes = 5  # По умолчанию 5 минут
            text_parts = args[1:]

            # Проверяем последний аргумент на число
            if text_parts[-1].isdigit():
                interval_minutes = int(text_parts[-1])
                text = ' '.join(text_parts[:-1])
            else:
                text = ' '.join(text_parts)

            self.command_piar(peer_id, user_id, text, interval_minutes, chat_id)

        else:
            # Ищем похожие команды
            similar = self.get_similar_commands(original_command)
            if similar:
                similar_text = ', '.join(similar)
                self.send_message(peer_id, f'🤔 Команда "/{original_command}" не найдена. Возможно, вы имели в виду: {similar_text}')
            else:
                self.send_message(peer_id, f'❌ Команда "/{original_command}" не найдена.')


    def process_message(self, event):
        # Обработка callback-событий от inline-кнопок
        if event['type'] == 'message_event':
            self.handle_callback(event)
            return

        # Обработка события добавления бота в беседу
        if event['type'] == 'message_new':
            message = event['object']['message']
            action = message.get('action', {})

            # Проверяем, был ли бот добавлен в беседу
            if action.get('type') == 'chat_invite_user' and action.get('member_id') == -self.group_id:
                peer_id = message.get('peer_id')
                chat_id = peer_id - 2000000000 if peer_id > 2000000000 else None

                if chat_id:
                    self.handle_bot_invited_to_chat(peer_id, chat_id)
                return

            # Проверяем, был ли пользователь добавлен в беседу
            if action.get('type') == 'chat_invite_user':
                invited_user_id = action.get('member_id')
                peer_id = message.get('peer_id')
                chat_id = peer_id - 2000000000 if peer_id > 2000000000 else None

                if invited_user_id and invited_user_id > 0 and chat_id:
                    self.check_user_ban_on_invite(peer_id, chat_id, invited_user_id)
                return

        if event['type'] != 'message_new':
            return

        message = event['object']['message']
        text = message.get('text', '').strip()
        user_id = message.get('from_id')
        peer_id = message.get('peer_id')
        payload = message.get('payload')

        # Определяем ID чата (если это групповая беседа)
        chat_id = peer_id - 2000000000 if peer_id > 2000000000 else None

        # Обработка нажатий на кнопки (payload)
        if payload:
            try:
                payload_data = json.loads(payload)
                if payload_data.get('action') == 'join_dice':
                    game_id = payload_data.get('game_id')
                    self.handle_dice_join(peer_id, user_id, game_id, chat_id)
                    return
                elif payload_data.get('action') == 'cancel_dice':
                    game_id = payload_data.get('game_id')
                    self.handle_dice_cancel(peer_id, user_id, game_id, chat_id)
                    return
            except Exception as e:
                self.log(f"Ошибка обработки payload: {e}")

        if not text or not user_id:
            return

        # Увеличиваем счетчик сообщений
        try:
            self.db.increment_message_count(user_id)
        except Exception as e:
            self.log(f"Ошибка обновления счетчика сообщений: {e}")

        # Проверка мута пользователя
        if chat_id:
            try:
                active_mute = self.db.get_active_mute_in_chat(user_id, chat_id)
                if active_mute:
                    # Пользователь замучен, удаляем сообщение
                    try:
                        message_id = message.get('conversation_message_id')
                        if message_id:
                            self.api_request('messages.delete', {
                                'peer_id': peer_id,
                                'cmids': [message_id],
                                'delete_for_all': 1
                            })
                    except Exception as e:
                        self.log(f"Ошибка удаления сообщения от замученного пользователя: {e}")
                    return
            except Exception as e:
                self.log(f"Ошибка проверки мута: {e}")

            # Проверка на запрещенные слова
            try:
                filtered_word = self.db.check_message_for_filtered_words(chat_id, text)
                if filtered_word:
                    # Сообщение содержит запрещенное слово, пытаемся удалить его
                    message_id = message.get('conversation_message_id')
                    if message_id:
                        result = self.api_request('messages.delete', {
                            'peer_id': peer_id,
                            'cmids': [message_id],
                            'delete_for_all': 1
                        })

                        # Проверяем, удалось ли удалить сообщение
                        if result is not None:
                            # Успешно удалено
                            warning_text = "📛 Сообщение было удалено, это запрещенное слово в чате.\n\n"
                            warning_text += "📝 Список всех запретов можно посмотреть: /filter list"
                            self.send_message(peer_id, warning_text)
                            self.log(f"Удалено сообщение от пользователя {user_id} в чате {chat_id} за использование запрещенного слова: {filtered_word}")
                        else:
                            # Не удалось удалить (вероятно, админ или владелец)
                            self.log(f"Не удалось удалить сообщение от пользователя {user_id} в чате {chat_id} с запрещенным словом '{filtered_word}' (возможно, пользователь является администратором)")
                    return
            except Exception as e:
                self.log(f"Ошибка проверки фильтра слов: {e}")

        # Получаем информацию о пользователе
        user_info = self.get_user_info(user_id)
        username = user_info['screen_name'] if user_info else str(user_id)

        # Обработка команд
        if text.startswith(('/', '!')):
            self.handle_command(text, user_id, username, peer_id, chat_id, message)
        else:
            # Обработка команд без слэша (краш и ставка)
            self.handle_commands_without_slash(text, user_id, username, peer_id, chat_id, message)


    def handle_commands_without_slash(self, text, user_id, username, peer_id, chat_id, message):
        """Обрабатывает команды без слэша (краш и ставка)"""
        text_lower = text.lower().strip()
        words = text.split()

        if len(words) < 2:
            return

        # Обработка команды "Краш"
        if text_lower.startswith('краш '):
            try:
                multiplier = words[1]
                amount = ' '.join(words[2:]) if len(words) > 2 else '100'
                self.command_crash(peer_id, user_id, multiplier, amount, chat_id)
            except Exception as e:
                self.log(f"Ошибка обработки команды краш без слэша: {e}")

        # Обработка команды "Ставка"
        elif text_lower.startswith('ставка '):
            try:
                if len(words) >= 3:
                    bet_type = words[1]
                    bet_target = words[2] if len(words) > 2 else None
                    amount = ' '.join(words[3:]) if len(words) > 3 else '100'

                    # Обрабатываем разные типы ставок
                    if bet_type in ['чет', 'четное', 'even']:
                        self.command_bet(peer_id, user_id, 'чет', amount, None, chat_id)
                    elif bet_type in ['нечет', 'нечетное', 'odd']:
                        self.command_bet(peer_id, user_id, 'нечет', amount, None, chat_id)
                    elif bet_type in ['красное', 'крас', 'red']:
                        self.command_bet(peer_id, user_id, 'красное', amount, None, chat_id)
                    elif bet_type in ['черное', 'черн', 'black']:
                        self.command_bet(peer_id, user_id, 'черное', amount, None, chat_id)
                    else:
                        # Попытка распознать как число
                        try:
                            number = int(bet_type)
                            if 0 <= number <= 36:
                                self.command_bet(peer_id, user_id, 'число', amount, bet_type, chat_id)
                        except ValueError:
                            pass
            except Exception as e:
                self.log(f"Ошибка обработки команды ставка без слэша: {e}")

        # Обработка команды "Дрим"
        elif text_lower.startswith('дрим '):
            try:
                multiplier = words[1]
                amount = ' '.join(words[2:]) if len(words) > 2 else '100'
                self.command_dream(peer_id, user_id, multiplier, amount, chat_id)
            except Exception as e:
                self.log(f"Ошибка обработки команды дрим без слэша: {e}")

    def handle_dice_join(self, peer_id, user_id, game_id, chat_id):
        """Обрабатывает нажатие на кнопку 'Играть' в игре в кости"""
        if not game_id:
            return

        try:
            # Подключаемся к игре через команду
            self.command_dice(peer_id, user_id, [str(game_id)], chat_id)
        except Exception as e:
            self.log(f"Ошибка при обработке нажатия кнопки 'Играть': {e}")
            self.send_message(peer_id, '❌ Ошибка при попытке присоединиться к игре.')

    def handle_dice_cancel(self, peer_id, user_id, game_id, chat_id):
        """Обрабатывает нажатие на кнопку 'Отменить' в игре в кости"""
        if not game_id:
            return

        try:
            game = self.db.get_dice_game(game_id)
            if not game:
                self.send_message(peer_id, '❌ Игра не найдена!')
                return

            if game['creator_id'] != user_id:
                self.send_message(peer_id, '❌ Только создатель игры может ее отменить!')
                return

            if game['status'] != 'waiting':
                self.send_message(peer_id, '❌ Игра уже началась или была отменена!')
                return

            # Отменяем игру
            self.db.cancel_dice_game(game_id)

            # Возвращаем ставки
            players = self.db.get_dice_players(game_id)
            self.db.update_user_balance(game['creator_id'], game['bet_amount'])
            for player in players:
                self.db.update_user_balance(player['user_id'], game['bet_amount'])

            # Отменяем таймер
            global DICE_TIMERS
            if game_id in DICE_TIMERS:
                DICE_TIMERS[game_id].cancel()
                del DICE_TIMERS[game_id]

            amount_display = self.format_number(game['bet_amount'])
            self.send_message(peer_id, f'❌ Игра в кости №{game_id} отменена создателем. Ставки ({amount_display}$) возвращены всем участникам.')

        except Exception as e:
            self.log(f"Ошибка при обработке нажатия кнопки 'Отменить': {e}")
            self.send_message(peer_id, '❌ Ошибка при отмене игры.')

    def handle_callback(self, event):
        """Обрабатывает callback-события от inline-кнопок"""
        try:
            event_data = event['object']
            user_id = event_data['user_id']
            peer_id = event_data['peer_id']
            event_id = event_data['event_id']

            # Payload может быть как строкой, так и словарем
            payload = event_data.get('payload', {})
            if isinstance(payload, str):
                payload = json.loads(payload)

            action = payload.get('action')

            # Определяем chat_id
            chat_id = peer_id - 2000000000 if peer_id > 2000000000 else None

            if action == 'activate_chat':
                # Проверяем, не активирована ли уже беседа
                if chat_id and self.is_chat_registered(chat_id):
                    # Отправляем ответ на callback
                    self.api_request('messages.sendMessageEventAnswer', {
                        'event_id': event_id,
                        'user_id': user_id,
                        'peer_id': peer_id,
                        'event_data': json.dumps({
                            'type': 'show_snackbar',
                            'text': 'Беседа уже активирована!'
                        })
                    })
                    return

                # Проверяем права пользователя перед активацией
                admin_rights = self.check_user_admin_rights(user_id, chat_id)
                if not admin_rights['is_admin'] and not admin_rights['is_owner']:
                    # Отправляем ответ на callback с ошибкой
                    self.api_request('messages.sendMessageEventAnswer', {
                        'event_id': event_id,
                        'user_id': user_id,
                        'peer_id': peer_id,
                        'event_data': json.dumps({
                            'type': 'show_snackbar',
                            'text': '❌ Только администратор или создатель может активировать бота!'
                        })
                    })
                    return

                # Активация беседы через inline-кнопку
                self.command_start(peer_id, user_id, chat_id)

                # Отправляем ответ на callback только если активация успешна
                self.api_request('messages.sendMessageEventAnswer', {
                    'event_id': event_id,
                    'user_id': user_id,
                    'peer_id': peer_id,
                    'event_data': json.dumps({
                        'type': 'show_snackbar',
                        'text': 'Беседа активирована!'
                    })
                })

            elif action == 'ban_forever':
                # Обработка бана навсегда
                target_user_id = payload.get('user_id')
                target_chat_id = payload.get('chat_id')
                ban_reason = payload.get('reason', 'Серьезное нарушение правил')

                if not target_user_id or not target_chat_id:
                    self.api_request('messages.sendMessageEventAnswer', {
                        'event_id': event_id,
                        'user_id': user_id,
                        'peer_id': peer_id,
                        'event_data': json.dumps({
                            'type': 'show_snackbar',
                            'text': '❌ Ошибка данных!'
                        })
                    })
                    return

                # Проверяем права пользователя
                moderation_check = self.can_moderate_user(user_id, target_user_id, target_chat_id)
                if not moderation_check['can_moderate']:
                    self.api_request('messages.sendMessageEventAnswer', {
                        'event_id': event_id,
                        'user_id': user_id,
                        'peer_id': peer_id,
                        'event_data': json.dumps({
                            'type': 'show_snackbar',
                            'text': '⛔️ Отказано! Доступ к команде /ban запрещен!'
                        })
                    })
                    return

                # Баним пользователя навсегда
                self.db.add_chat_ban(target_user_id, target_chat_id, ban_reason, user_id)

                target_info = self.get_user_info(target_user_id)
                target_name = target_info['screen_name'] if target_info else str(target_user_id)

                # Отправляем уведомление о бане
                ban_message = f'🚫 [id{target_user_id}|Пользователь] заблокирован в чате до бессрочно.\n⚠️ Причина: {ban_reason}'

                # Создаем кнопку для разбана
                keyboard = {
                    "inline": True,
                    "buttons": [
                        [{
                            "action": {
                                "type": "callback",
                                "label": "🔴 Снять блокировку",
                                "payload": json.dumps({
                                    "action": "unban_user",
                                    "user_id": target_user_id,
                                    "chat_id": target_chat_id
                                })
                            },
                            "color": "negative"
                        }]
                    ]
                }

                self.send_message(peer_id, ban_message, json.dumps(keyboard))

                # Отправляем ответ на callback
                self.api_request('messages.sendMessageEventAnswer', {
                    'event_id': event_id,
                    'user_id': user_id,
                    'peer_id': peer_id,
                    'event_data': json.dumps({
                        'type': 'show_snackbar',
                        'text': f'🚫 @{target_name} забанен навсегда!'
                    })
                })

            elif action == 'unban_user':
                # Обработка снятия блокировки
                target_user_id = payload.get('user_id')
                target_chat_id = payload.get('chat_id')

                if not target_user_id or not target_chat_id:
                    self.api_request('messages.sendMessageEventAnswer', {
                        'event_id': event_id,
                        'user_id': user_id,
                        'peer_id': peer_id,
                        'event_data': json.dumps({
                            'type': 'show_snackbar',
                            'text': '❌ Ошибка данных!'
                        })
                    })
                    return

                # Проверяем права пользователя
                user_role = self.get_user_role(user_id, target_chat_id)
                if user_role['level'] < 20:
                    self.api_request('messages.sendMessageEventAnswer', {
                        'event_id': event_id,
                        'user_id': user_id,
                        'peer_id': peer_id,
                        'event_data': json.dumps({
                            'type': 'show_snackbar',
                            'text': '❌ У вас нет прав для разбана!'
                        })
                    })
                    return

                # Снимаем блокировку
                user_ban = self.db.get_user_ban_in_chat(target_user_id, target_chat_id)
                if user_ban:
                    self.db.remove_chat_ban(target_user_id, target_chat_id)

                    target_info = self.get_user_info(target_user_id)
                    target_name = target_info['screen_name'] if target_info else str(target_user_id)

                    # Отправляем уведомление о разбане
                    self.send_message(peer_id, f'✅ [id{target_user_id}|Пользователь] разблокирован в чате.')

                    # Отправляем ответ на callback
                    self.api_request('messages.sendMessageEventAnswer', {
                        'event_id': event_id,
                        'user_id': user_id,
                        'peer_id': peer_id,
                        'event_data': json.dumps({
                            'type': 'show_snackbar',
                            'text': f'✅ @{target_name} разблокирован!'
                        })
                    })
                else:
                    self.api_request('messages.sendMessageEventAnswer', {
                        'event_id': event_id,
                        'user_id': user_id,
                        'peer_id': peer_id,
                        'event_data': json.dumps({
                            'type': 'show_snackbar',
                            'text': '❌ Пользователь не заблокирован!'
                        })
                    })

        except Exception as e:
            self.log(f"Ошибка обработки callback: {e}")

    def run(self):
        if not self.get_long_poll_server():
            self.log("Ошибка получения Long Poll сервера")
            return

        self.log("Бот запущен и слушает сообщения...")

        while True:
            try:
                params = {
                    'act': 'a_check',
                    'key': self.key,
                    'ts': self.ts,
                    'wait': 25
                }

                if not self.server:
                    self.log("Сервер Long Poll не инициализирован")
                    break

                response = requests.get(self.server, params=params, timeout=30)
                data = response.json()

                if 'failed' in data:
                    if data['failed'] == 1:
                        self.ts = data['ts']
                    else:
                        if not self.get_long_poll_server():
                            self.log("Ошибка обновления Long Poll сервера")
                            break

                if 'updates' in data:
                    if data['updates']:
                        self.log(f"Получено событий: {len(data['updates'])}")
                    for update in data['updates']:
                        self.log(f"Обработка события: {update.get('type', 'unknown')}")
                        threading.Thread(target=self.process_message, args=(update,)).start()

                self.ts = data['ts']

            except Exception as e:
                self.log(f"Ошибка в главном цикле: {e}")
                time.sleep(5)

    def convert_number_to_short(self, number_str):
        """
        Конвертирует числовую строку в короткий формат (например, 1000000 -> 1кк).
        """
        try:
            number = int(number_str)
            if number >= 1000000000:
                return f"{number / 1000000000:.2f}млд"
            elif number >= 1000000:
                return f"{number / 1000000:.2f}кк"
            elif number >= 1000:
                return f"{number / 1000:.2f}к"
            else:
                return f"{number}"
        except ValueError:
            return None

if __name__ == "__main__":
    try:
        bot = VKBot()
        bot.run()
    except KeyboardInterrupt:
        print("\n🛑 Остановка по запросу пользователя...")
        if 'bot' in locals():
            bot.stop()
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        if 'bot' in locals():
            bot.stop()
    bot = None
    try:
        bot = VKBot()

        # Получаем ID гранд-менеджера
        response = bot.api_request('utils.resolveScreenName', {
            'screen_name': CONFIG['grand_manager']
        })

        if response and response.get('type') == 'user':
            GRAND_MANAGER_ID = response['object_id']
            bot.log(f"ID гранд-менеджера: {GRAND_MANAGER_ID}")

            # Автоматически назначаем основателю права системного администратора уровня 4
            try:
                existing_admin = bot.db.get_system_admin(GRAND_MANAGER_ID)
                if not existing_admin:
                    bot.db.add_system_admin(GRAND_MANAGER_ID, CONFIG['grand_manager'], 4, GRAND_MANAGER_ID)
                    bot.log(f"Системные права основателя бота автоматически назначены")
                elif existing_admin['access_level'] < 4:
                    bot.db.add_system_admin(GRAND_MANAGER_ID, CONFIG['grand_manager'], 4, GRAND_MANAGER_ID)
                    bot.log(f"Права системного администратора обновлены до уровня 4")
            except Exception as e:
                bot.log(f"Ошибка назначения системных прав: {e}")

        # Автоматически назначаем angel_sozb права разработчика (уровень 5)
        developer_response = bot.api_request('utils.resolveScreenName', {
            'screen_name': 'angel_sozb'
        })

        if developer_response and developer_response.get('type') == 'user':
            developer_id = developer_response['object_id']
            bot.log(f"ID разработчика angel_sozb: {developer_id}")

            try:
                existing_dev = bot.db.get_system_admin(developer_id)
                if not existing_dev:
                    bot.db.add_system_admin(developer_id, 'angel_sozb', 5, GRAND_MANAGER_ID or developer_id)
                    bot.log(f"Системные права разработчика angel_sozb автоматически назначены")
                elif existing_dev['access_level'] < 5:
                    bot.db.add_system_admin(developer_id, 'angel_sozb', 5, GRAND_MANAGER_ID or developer_id)
                    bot.log(f"Права разработчика angel_sozb обновлены до уровня 5")
            except Exception as e:
                bot.log(f"Ошибка назначения прав разработчика: {e}")

        bot.run()
    except KeyboardInterrupt:
        print("\nБот остановлен пользователем")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
    finally:
        try:
            if bot:
                bot.db.close()
        except:
            pass
