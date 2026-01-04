"""
JPX400銘柄リスト管理モジュール

JPX400に含まれる銘柄リストを管理します。

Copyright (c) 2025 JPX400スクリーニングシステム

This software is licensed under the MIT License.
See LICENSE file for details.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.utils.console import setup_console_encoding
setup_console_encoding()

import json
import csv
from pathlib import Path
from typing import List, Optional


class JPX400Manager:
    """JPX400銘柄リスト管理クラス"""
    
    def __init__(self, list_file: str = "data/jpx400_symbols.json"):
        """
        初期化
        
        Args:
            list_file: 銘柄リストファイルのパス（JSON形式）
        """
        self.list_file = Path(list_file)
        self.list_file.parent.mkdir(parents=True, exist_ok=True)
    
    def load_symbols(self) -> List[str]:
        """
        JPX400銘柄リストを読み込む
        
        Returns:
            List[str]: 銘柄コードのリスト
        """
        if not self.list_file.exists():
            print(f"[JPX400Manager] 銘柄リストファイルが見つかりません: {self.list_file}")
            print("  デフォルトの銘柄リストを作成します")
            self._create_default_list()
        
        # 複数のエンコーディングを試行
        encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932', 'euc-jp']
        
        for encoding in encodings:
            try:
                with open(self.list_file, 'r', encoding=encoding) as f:
                    data = json.load(f)
                    symbols = data.get('symbols', [])
                    print(f"[JPX400Manager] {len(symbols)}銘柄を読み込みました (エンコーディング: {encoding})")
                    return symbols
            except UnicodeDecodeError:
                # エンコーディングが合わない場合は次を試行
                continue
            except json.JSONDecodeError as e:
                # JSONのパースエラーは別のエンコーディングを試行
                continue
            except Exception as e:
                # その他のエラーは最後のエンコーディングまで試行
                if encoding == encodings[-1]:
                    print(f"[JPX400Manager] 銘柄リストの読み込みエラー: {e}")
                    return []
                continue
        
        print(f"[JPX400Manager] すべてのエンコーディングで読み込みに失敗しました")
        return []
    
    def save_symbols(self, symbols: List[str], metadata: Optional[dict] = None):
        """
        JPX400銘柄リストを保存
        
        Args:
            symbols: 銘柄コードのリスト
            metadata: メタデータ（更新日時など）
        """
        data = {
            'symbols': symbols,
            'count': len(symbols),
            'metadata': metadata or {}
        }
        
        if 'updated_at' not in data['metadata']:
            from datetime import datetime
            data['metadata']['updated_at'] = datetime.now().isoformat()
        
        try:
            with open(self.list_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"[JPX400Manager] {len(symbols)}銘柄を保存しました: {self.list_file}")
        except Exception as e:
            print(f"[JPX400Manager] 銘柄リストの保存エラー: {e}")
    
    def load_from_csv(self, csv_file: str) -> List[str]:
        """
        CSVファイルから銘柄リストを読み込む
        
        Args:
            csv_file: CSVファイルのパス
            
        Returns:
            List[str]: 銘柄コードのリスト
        """
        symbols = []
        csv_path = Path(csv_file)
        
        if not csv_path.exists():
            print(f"[JPX400Manager] CSVファイルが見つかりません: {csv_file}")
            return []
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # ヘッダー行をスキップ
                
                for row in reader:
                    if row and row[0].strip():
                        symbol = row[0].strip()
                        # 4桁の数字か確認
                        if symbol.isdigit() and len(symbol) == 4:
                            symbols.append(symbol)
            
            print(f"[JPX400Manager] CSVから{len(symbols)}銘柄を読み込みました")
            return symbols
        except Exception as e:
            print(f"[JPX400Manager] CSVファイルの読み込みエラー: {e}")
            return []
    
    def _create_default_list(self):
        """
        デフォルトの銘柄リストを作成（サンプル）
        
        注意: 実際のJPX400銘柄リストは手動で更新する必要があります
        """
        # サンプルとして主要銘柄を追加
        default_symbols = [
            "7203",  # トヨタ自動車
            "6758",  # ソニーグループ
            "9432",  # NTT
            "9984",  # ソフトバンクグループ
            "8035",  # 東京エレクトロン
            "6861",  # キーエンス
            "6098",  # リクルートホールディングス
            "4063",  # 信越化学工業
            "4519",  # 中外製薬
            "6501",  # 日立製作所
        ]
        
        self.save_symbols(default_symbols, {
            'note': 'これはサンプルリストです。実際のJPX400銘柄リストに更新してください。'
        })
        
        print("[JPX400Manager] デフォルトの銘柄リストを作成しました")
        print("  実際のJPX400銘柄リストに更新することを推奨します")


if __name__ == '__main__':
    # テスト実行
    manager = JPX400Manager()
    symbols = manager.load_symbols()
    print(f"読み込んだ銘柄数: {len(symbols)}")
    if symbols:
        print(f"最初の10銘柄: {symbols[:10]}")

