"""
スコア保存・検証・レポート生成

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""

import json
import sqlite3
from datetime import datetime
from typing import Dict, Optional

import pandas as pd

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None


class SentimentEvaluator:
    """センチメントスコアの保存・評価クラス"""

    def __init__(self, db_path: str = "data/tick_data.db"):
        self.db_path = db_path
        self._ensure_tables()

    # -----------------------------
    # パブリックAPI
    # -----------------------------
    def save_score(self, date_str: str, sentiment: Dict) -> None:
        """スコアをDBに保存"""
        weights_version = sentiment.get("weights_version", 1)
        weights_json = sentiment.get("weights_json")  # 重み付けパラメータ
        
        with self._conn() as conn:
            # スコアを保存
            conn.execute(
                """
                INSERT OR REPLACE INTO sentiment_scores
                (date, calculated_at, total_score, sentiment_level, indicators_json, weights_version, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    date_str,
                    sentiment.get("calculated_at") or datetime.now().isoformat(),
                    sentiment.get("score", 0.0),
                    sentiment.get("level"),
                    json.dumps(sentiment.get("indicators", {})),
                    weights_version,
                    datetime.now().isoformat(),
                ),
            )
            
            # 重み付けパラメータを保存（存在する場合）
            if weights_json:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO sentiment_weights
                    (version, weights_json, description, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        weights_version,
                        weights_json,
                        sentiment.get("weights_description", ""),
                        datetime.now().isoformat(),
                        datetime.now().isoformat(),
                    ),
                )

    def get_latest_weights(self) -> Optional[Dict]:
        """最新の重み付けパラメータを取得"""
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM sentiment_weights ORDER BY version DESC LIMIT 1"
            ).fetchone()
            if not row:
                return None
            try:
                weights = json.loads(row["weights_json"])
            except Exception:
                return None
            return {
                "version": row["version"],
                "weights": weights,
                "description": row["description"] if "description" in row.keys() else "",
            }

    def record_market_outcome(self, date_str: str) -> None:
        """実際の市場動向を記録（日経平均）"""
        self._ensure_yfinance()
        df = self._fetch_history("^N225", period="5d", interval="1d")
        if df is None or df.empty:
            print("[WARN] 市場データ取得に失敗したため記録をスキップしました")
            return

        # 日付で該当行を取得
        target = df.loc[df.index.date == datetime.fromisoformat(date_str).date()]
        if target.empty:
            # 最新行をフォールバック
            target = df.tail(1)

        row = target.iloc[-1]
        prev_close = df["close"].iloc[-2] if len(df) >= 2 else row["close"]
        today_close = row["close"]
        change_pct = (today_close / prev_close - 1) * 100 if prev_close else 0.0

        market_strength = self._calc_market_strength(change_pct)
        direction = "上昇" if change_pct > 0 else "下落" if change_pct < 0 else "横ばい"

        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO market_outcomes
                (date, nikkei225_open, nikkei225_close, nikkei225_high, nikkei225_low,
                 nikkei225_change_pct, nikkei225_change_abs, market_direction, market_strength,
                 is_positive, is_strong_positive, is_strong_negative, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    date_str,
                    row.get("open"),
                    row.get("close"),
                    row.get("high"),
                    row.get("low"),
                    change_pct,
                    today_close - prev_close,
                    direction,
                    market_strength,
                    1 if change_pct > 0 else 0,
                    1 if change_pct >= 1.0 else 0,
                    1 if change_pct <= -1.0 else 0,
                    datetime.now().isoformat(),
                    datetime.now().isoformat(),
                ),
            )

    def evaluate_scores(self, score_date_str: str, outcome_date_str: Optional[str] = None) -> None:
        """
        スコアを評価し、score_evaluationsに保存
        
        Args:
            score_date_str: 評価するスコアの日付（通常は前日）
            outcome_date_str: 比較する市場動向の日付（Noneの場合はscore_date_strと同じ）
        """
        if outcome_date_str is None:
            outcome_date_str = score_date_str
            
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            score = conn.execute(
                """
                SELECT * FROM sentiment_scores
                WHERE date = ?
                ORDER BY calculated_at DESC
                LIMIT 1
                """,
                (score_date_str,),
            ).fetchone()

            outcome = conn.execute(
                """
                SELECT * FROM market_outcomes
                WHERE date = ?
                """,
                (outcome_date_str,),
            ).fetchone()

            if not score:
                print(f"[WARN] {score_date_str}のスコアが見つかりません")
                return
            if not outcome:
                print(f"[WARN] {outcome_date_str}の市場データが見つかりません")
                return

            predicted_score = score["total_score"]
            actual_strength = outcome["market_strength"]
            normalized_predicted = (predicted_score - 50) * 2
            prediction_error = abs(normalized_predicted - actual_strength)

            direction_match = 1 if ((normalized_predicted > 0 and actual_strength > 0) or (normalized_predicted < 0 and actual_strength < 0)) else 0
            is_correct_positive = 1 if (predicted_score >= 60 and actual_strength > 0) else 0
            is_correct_negative = 1 if (predicted_score <= 40 and actual_strength < 0) else 0
            is_false_positive = 1 if (predicted_score >= 60 and actual_strength < 0) else 0
            is_false_negative = 1 if (predicted_score <= 40 and actual_strength > 0) else 0

            conn.execute(
                """
                INSERT OR REPLACE INTO score_evaluations
                (score_id, outcome_id, date, predicted_score, actual_market_strength,
                 prediction_error, direction_match, is_correct_positive, is_correct_negative,
                 is_false_positive, is_false_negative, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    score["id"],
                    outcome["id"],
                    outcome_date_str,  # 評価日は市場動向の日付を使用
                    predicted_score,
                    actual_strength,
                    prediction_error,
                    direction_match,
                    is_correct_positive,
                    is_correct_negative,
                    is_false_positive,
                    is_false_negative,
                    datetime.now().isoformat(),
                ),
            )

    def generate_evaluation_report(self, days: int = 30) -> Dict:
        """評価レポート（方向性一致率・平均誤差など）"""
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            stats = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(direction_match) AS matches,
                    AVG(prediction_error) AS avg_error,
                    SUM(is_correct_positive) AS tp,
                    SUM(is_false_positive) AS fp,
                    SUM(is_false_negative) AS fn
                FROM score_evaluations
                WHERE date >= date('now', '-' || ? || ' days')
                """,
                (days,),
            ).fetchone()

        total = stats["total"] or 0
        matches = stats["matches"] or 0
        avg_error = stats["avg_error"] or 0.0
        tp = stats["tp"] or 0
        fp = stats["fp"] or 0
        fn = stats["fn"] or 0

        direction_accuracy = (matches / total * 100) if total else 0.0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0

        return {
            "direction_accuracy": direction_accuracy,
            "avg_error": avg_error,
            "precision": precision,
            "recall": recall,
            "sample_size": total,
        }

    def optimize_weights(
        self,
        days: int = 30,
        step: float = 0.2,
        min_weight: float = 0.0,
        max_weight: float = 2.0,
        description: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        評価結果を元に重みを自動調整し、新しいバージョンを保存

        ロジック（簡易ルールベース）:
        - 過去days日間の評価で方向性が外れたケースのみペナルティを与える
        - 外れた予測で「予測と同じ向きの重み付きスコア」を持つ指標を減衰
        - 減衰率: weight * (1 - step * (penalty / 総ペナルティ))
        - 重みは[min_weight, max_weight]でクリップ
        """
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row

            # 最新の重みを取得
            latest = conn.execute(
                "SELECT version, weights_json FROM sentiment_weights ORDER BY version DESC LIMIT 1"
            ).fetchone()
            if not latest:
                print("[WARN] 重みが保存されていないため最適化をスキップします。")
                return None

            base_version = latest["version"]
            weights = json.loads(latest["weights_json"])

            rows = conn.execute(
                """
                SELECT se.direction_match, se.predicted_score, se.actual_market_strength, ss.indicators_json
                FROM score_evaluations se
                JOIN sentiment_scores ss ON se.score_id = ss.id
                WHERE se.date >= date('now', '-' || ? || ' days')
                """,
                (days,),
            ).fetchall()

            if not rows:
                print(f"[INFO] 過去{days}日間に評価データがないため最適化をスキップします。")
                return None

            penalties: Dict[str, float] = {}
            total_evals = len(rows)
            for row in rows:
                try:
                    indicators = json.loads(row["indicators_json"])
                except Exception:
                    continue

                # 方向性が外れたケースのみペナルティ
                if row["direction_match"] == 0:
                    pred_sign = 1 if row["predicted_score"] >= 50 else -1
                    for name, data in indicators.items():
                        ws = data.get("weighted_score")
                        if ws is None:
                            score_val = data.get("score")
                            weight_val = data.get("weight", 1.0)
                            ws = score_val * weight_val if score_val is not None else None
                        if ws is None or ws == 0:
                            continue
                        sign_ws = 1 if ws > 0 else -1
                        # 予測と同じ向きに寄与した指標を減衰対象にする
                        if sign_ws == pred_sign:
                            penalties[name] = penalties.get(name, 0.0) + abs(ws)

            if not penalties:
                print("[INFO] ペナルティ対象がなかったため最適化をスキップします。")
                return None

            total_penalty = sum(penalties.values())
            new_weights = dict(weights)

            for name, weight in weights.items():
                factor = penalties.get(name, 0.0) / total_penalty if total_penalty > 0 else 0.0
                adjusted = weight * (1 - step * factor)
                adjusted = max(min_weight, min(max_weight, adjusted))
                new_weights[name] = adjusted

            new_version = base_version + 1
            desc = description or f"auto-tuned from v{base_version} (last {days}d, step={step})"
            now = datetime.now().isoformat()

            conn.execute(
                """
                INSERT OR REPLACE INTO sentiment_weights
                (version, weights_json, description, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    new_version,
                    json.dumps(new_weights),
                    desc,
                    now,
                    now,
                ),
            )

            # 直近統計を返す
            report = self.generate_evaluation_report(days=days)
            return {
                "base_version": base_version,
                "new_version": new_version,
                "weights": new_weights,
                "report": report,
            }

    # -----------------------------
    # 内部処理
    # -----------------------------
    def _conn(self):
        return sqlite3.connect(self.db_path)

    def _ensure_tables(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sentiment_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    calculated_at TEXT NOT NULL,
                    total_score REAL NOT NULL,
                    sentiment_level TEXT,
                    indicators_json TEXT NOT NULL,
                    weights_version INTEGER DEFAULT 1,
                    created_at TEXT NOT NULL,
                    UNIQUE(date, calculated_at)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sentiment_date ON sentiment_scores(date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sentiment_weights_version ON sentiment_scores(weights_version)")

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_outcomes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    nikkei225_open REAL,
                    nikkei225_close REAL,
                    nikkei225_high REAL,
                    nikkei225_low REAL,
                    nikkei225_change_pct REAL,
                    nikkei225_change_abs REAL,
                    market_direction TEXT,
                    market_strength REAL,
                    is_positive INTEGER DEFAULT 0,
                    is_strong_positive INTEGER DEFAULT 0,
                    is_strong_negative INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT,
                    UNIQUE(date)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_date ON market_outcomes(date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_outcomes_direction ON market_outcomes(market_direction)")

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS score_evaluations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    score_id INTEGER NOT NULL,
                    outcome_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    predicted_score REAL NOT NULL,
                    actual_market_strength REAL NOT NULL,
                    prediction_error REAL NOT NULL,
                    direction_match INTEGER DEFAULT 0,
                    is_correct_positive INTEGER DEFAULT 0,
                    is_correct_negative INTEGER DEFAULT 0,
                    is_false_positive INTEGER DEFAULT 0,
                    is_false_negative INTEGER DEFAULT 0,
                    indicator_contributions_json TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(score_id, outcome_id),
                    FOREIGN KEY (score_id) REFERENCES sentiment_scores(id),
                    FOREIGN KEY (outcome_id) REFERENCES market_outcomes(id)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_eval_date ON score_evaluations(date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_eval_direction_match ON score_evaluations(direction_match)")

            # 重み付けパラメータテーブル
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sentiment_weights (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version INTEGER NOT NULL UNIQUE,
                    weights_json TEXT NOT NULL,
                    description TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT,
                    UNIQUE(version)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_weights_version ON sentiment_weights(version)")

    def _calc_market_strength(self, change_pct: float) -> float:
        if change_pct >= 1.0:
            return 100.0
        if change_pct >= 0.5:
            return 50.0
        if change_pct >= -0.5:
            return 0.0
        if change_pct >= -1.0:
            return -50.0
        return -100.0

    def _fetch_history(self, symbol: str, period: str, interval: str) -> Optional[pd.DataFrame]:
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=interval)
            if df is None or df.empty:
                return None
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.columns = [c.lower() for c in df.columns]
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            return df
        except Exception as e:  # pragma: no cover
            print(f"[WARN] {symbol}の取得に失敗: {e}")
            return None

    def _ensure_yfinance(self):
        if yf is None:
            raise ImportError("yfinance がインストールされていません。pip install yfinance を実行してください。")


