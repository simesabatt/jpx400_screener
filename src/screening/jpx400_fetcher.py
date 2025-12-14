"""
JPX400銘柄リスト取得モジュール

JPXの公式サイトからJPX400の構成銘柄リストを取得します。
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.utils.console import setup_console_encoding
setup_console_encoding()

import requests
import pandas as pd
from typing import List, Optional
from pathlib import Path
import re
import os


class JPX400Fetcher:
    """JPX400銘柄リスト取得クラス"""
    
    def __init__(self):
        """初期化"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def fetch_from_jpx_pdf_url(self, pdf_url: Optional[str] = None) -> Optional[List[str]]:
        """
        JPXの公式サイトからPDFをダウンロードして銘柄リストを取得
        
        Args:
            pdf_url: PDFのURL（Noneの場合は最新のURLを推測）
            
        Returns:
            List[str]: 銘柄コードのリスト、またはNone（取得失敗時）
        """
        try:
            # PDFをダウンロードして解析
            # 注意: PDFの解析にはPyPDF2やpdfplumberなどのライブラリが必要
            
            if pdf_url is None:
                # 最新のPDFのURLを推測（実際のURLはJPXの公式サイトで確認が必要）
                # 例: https://www.jpx.co.jp/markets/indices/line-up/files/400_j.pdf
                pdf_url = "https://www.jpx.co.jp/markets/indices/jpx-nikkei400/tvdivq00000031dd-att/400_j.pdf"
            
            print(f"[JPX400Fetcher] PDFをダウンロード中: {pdf_url}")
            
            response = self.session.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            # PDFを一時ファイルに保存
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                tmp_file.write(response.content)
                tmp_path = tmp_file.name
            
            # PDFを解析
            symbols = self._parse_pdf(tmp_path)
            
            # 一時ファイルを削除
            os.unlink(tmp_path)
            
            if symbols:
                print(f"[JPX400Fetcher] PDFから{len(symbols)}銘柄を抽出しました")
                return symbols
            else:
                print("[JPX400Fetcher] PDFから銘柄コードを抽出できませんでした")
                return None
        
        except requests.exceptions.RequestException as e:
            print(f"[JPX400Fetcher] PDFダウンロードエラー: {e}")
            print(f"  URL: {pdf_url}")
            return None
        except Exception as e:
            print(f"[JPX400Fetcher] PDF取得エラー: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _parse_pdf(self, pdf_path: str) -> List[str]:
        """
        PDFファイルから銘柄コードを抽出
        
        Args:
            pdf_path: PDFファイルのパス
            
        Returns:
            List[str]: 銘柄コードのリスト
        """
        symbols = []
        
        try:
            # pdfplumberを使用（推奨）
            try:
                import pdfplumber
                
                with pdfplumber.open(pdf_path) as pdf:
                    for page_num, page in enumerate(pdf.pages, 1):
                        # テーブル構造を優先的に解析
                        tables = page.extract_tables()
                        if tables:
                            for table in tables:
                                for row in table:
                                    if row:
                                        for cell in row:
                                            if cell:
                                                cell_text = str(cell).strip()
                                                # 4桁の数字（銘柄コード）を抽出
                                                pattern = r'\b\d{4}\b'
                                                matches = re.findall(pattern, cell_text)
                                                for match in matches:
                                                    code = int(match)
                                                    if 1000 <= code <= 9999:
                                                        symbols.append(match)
                        
                        # テーブルがない場合や、テーブルから取得できなかった場合はテキストから抽出
                        text = page.extract_text()
                        if text:
                            # 4桁の数字（銘柄コード）を抽出
                            pattern = r'\b\d{4}\b'
                            matches = re.findall(pattern, text)
                            for match in matches:
                                # 年号やその他の4桁数字を除外（1000-9999の範囲で、一般的な銘柄コードの範囲）
                                code = int(match)
                                if 1000 <= code <= 9999:
                                    symbols.append(match)
                
                # 重複を削除
                symbols = list(set(symbols))
                symbols.sort()
                
                if symbols:
                    print(f"[JPX400Fetcher] PDFから{len(symbols)}銘柄を抽出しました（重複除去後）")
                
                return symbols
            
            except ImportError:
                # pdfplumberがインストールされていない場合、PyPDF2を試す
                try:
                    import PyPDF2
                    
                    with open(pdf_path, 'rb') as file:
                        pdf_reader = PyPDF2.PdfReader(file)
                        for page in pdf_reader.pages:
                            text = page.extract_text()
                            if text:
                                # 4桁の数字（銘柄コード）を抽出
                                pattern = r'\b\d{4}\b'
                                matches = re.findall(pattern, text)
                                for match in matches:
                                    code = int(match)
                                    if 1000 <= code <= 9999:
                                        symbols.append(match)
                    
                    # 重複を削除
                    symbols = list(set(symbols))
                    symbols.sort()
                    
                    return symbols
                
                except ImportError:
                    error_msg = "[JPX400Fetcher] PDF解析ライブラリがインストールされていません\n"
                    error_msg += "  以下のいずれかをインストールしてください:\n"
                    error_msg += "    pip install pdfplumber  （推奨）\n"
                    error_msg += "    または\n"
                    error_msg += "    pip install PyPDF2"
                    print(error_msg)
                    return []
        
        except Exception as e:
            print(f"[JPX400Fetcher] PDF解析エラー: {e}")
            return []
    
    def fetch_from_jpx_website(self) -> Optional[List[str]]:
        """
        JPXの公式サイトからJPX400銘柄リストを取得（自動）
        
        Returns:
            List[str]: 銘柄コードのリスト、またはNone（取得失敗時）
        """
        # PDF解析ライブラリがインストールされているか確認
        try:
            import pdfplumber
        except ImportError:
            try:
                import PyPDF2
            except ImportError:
                print("[JPX400Fetcher] PDF解析ライブラリがインストールされていません")
                print("  以下のコマンドでインストールしてください:")
                print("    pip install pdfplumber")
                return None
        
        # まずPDFから取得を試みる
        symbols = self.fetch_from_jpx_pdf_url()
        
        if symbols and len(symbols) >= 300:  # 300銘柄以上あれば有効と判断
            return symbols
        
        # PDFから取得できなかった場合、他の方法を試す
        print("[JPX400Fetcher] PDFからの取得に失敗したため、他の方法を試します")
        if symbols:
            print(f"[JPX400Fetcher] 取得できた銘柄数: {len(symbols)}件（300件未満のため失敗と判断）")
        return None
    
    def parse_csv_file(self, csv_file: str) -> List[str]:
        """
        CSVファイルからJPX400銘柄リストを解析
        
        Args:
            csv_file: CSVファイルのパス
            
        Returns:
            List[str]: 銘柄コードのリスト
        """
        symbols = []
        csv_path = Path(csv_file)
        
        if not csv_path.exists():
            print(f"[JPX400Fetcher] CSVファイルが見つかりません: {csv_file}")
            return []
        
        try:
            # CSVファイルを読み込み
            df = pd.read_csv(csv_path, encoding='utf-8')
            
            # 銘柄コード列を探す（様々な列名に対応）
            possible_columns = ['銘柄コード', 'コード', 'symbol', 'Symbol', 'SYMBOL', '銘柄', 'コード番号']
            
            code_column = None
            for col in possible_columns:
                if col in df.columns:
                    code_column = col
                    break
            
            if code_column is None:
                # 最初の列を使用
                code_column = df.columns[0]
                print(f"[JPX400Fetcher] 銘柄コード列が見つからないため、最初の列を使用: {code_column}")
            
            # 銘柄コードを抽出
            for value in df[code_column]:
                if pd.notna(value):
                    symbol = str(value).strip()
                    # 4桁の数字か確認
                    if symbol.isdigit() and len(symbol) == 4:
                        symbols.append(symbol)
            
            # 重複を削除
            symbols = list(set(symbols))
            symbols.sort()
            
            print(f"[JPX400Fetcher] CSVから{len(symbols)}銘柄を抽出しました")
            return symbols
        
        except Exception as e:
            print(f"[JPX400Fetcher] CSVファイルの解析エラー: {e}")
            return []
    
    def create_from_topix_constituents(self) -> List[str]:
        """
        TOPIX構成銘柄から主要銘柄を抽出（簡易版）
        
        注意: これはJPX400の正確なリストではありません
        実際のJPX400銘柄リストを使用することを推奨します
        
        Returns:
            List[str]: 銘柄コードのリスト
        """
        # TOPIX構成銘柄の主要銘柄（時価総額上位など）
        # これは簡易版であり、実際のJPX400とは異なります
        
        print("[JPX400Fetcher] 簡易版の銘柄リストを作成します")
        print("  注意: これはJPX400の正確なリストではありません")
        print("  実際のJPX400銘柄リストを使用することを推奨します")
        
        # 主要銘柄のリスト（時価総額上位など）
        # 実際のJPX400リストに置き換える必要があります
        major_symbols = [
            # 時価総額上位銘柄（例）
            "7203", "6758", "9432", "9984", "8035", "6861", "6098",
            "4063", "4519", "6501", "8058", "8306", "8411", "8766",
            # 以下、実際のJPX400リストに置き換える必要があります
        ]
        
        return major_symbols


if __name__ == '__main__':
    # テスト実行
    import sys
    from datetime import datetime
    
    fetcher = JPX400Fetcher()
    
    print("=" * 60)
    print("JPX400銘柄リスト取得テスト")
    print("=" * 60)
    print()
    
    # 方法1: JPX公式サイトから取得
    print("[テスト1] JPX公式サイトから取得を試みます...")
    print()
    symbols = fetcher.fetch_from_jpx_website()
    
    if symbols and len(symbols) >= 300:
        print(f"✅ 成功: {len(symbols)}銘柄を取得しました")
        print(f"   最初の10銘柄: {symbols[:10]}")
        print(f"   最後の10銘柄: {symbols[-10:]}")
        
        # 保存の確認
        save_choice = input("\n取得した銘柄リストを保存しますか？ (y/n): ")
        if save_choice.lower() == 'y':
            from src.screening.jpx400_manager import JPX400Manager
            manager = JPX400Manager()
            manager.save_symbols(symbols, {
                'source': 'JPX公式サイト（自動取得）',
                'updated_at': datetime.now().isoformat()
            })
            print(f"✅ 保存完了: data/jpx400_symbols.json")
    else:
        print(f"❌ 失敗: 取得できた銘柄数が少ないか、取得に失敗しました")
        print(f"   取得できた銘柄数: {len(symbols) if symbols else 0}")
        print()
        print("CSVファイルからの読み込みを試す場合は、以下を実行してください:")
        print("  python -c \"from src.screening.jpx400_fetcher import JPX400Fetcher; f = JPX400Fetcher(); s = f.parse_csv_file('data/jpx400_list.csv'); print(f'読み込んだ銘柄数: {len(s)}')\"")
    
    print()
    print("=" * 60)

