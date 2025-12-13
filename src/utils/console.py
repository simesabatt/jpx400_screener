"""
コンソール出力ユーティリティ

Windows環境での文字化け対策を含む
"""
import sys
import io
import platform

def setup_console_encoding():
    """
    コンソールのエンコーディングをUTF-8に設定
    Windowsでの文字化けを防ぐ
    
    注意: Tkinterアプリケーションでは標準出力が閉じられている可能性があるため、
    この関数は呼ばれても安全にスキップされます。
    """
    try:
        # Tkinterアプリケーションの場合はスキップ
        if 'tkinter' in sys.modules:
            return
        
        if platform.system() == 'Windows':
            # 標準出力・エラー出力が閉じられていないか確認
            if hasattr(sys.stdout, 'buffer') and not sys.stdout.closed:
                try:
                    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
                except (ValueError, AttributeError):
                    pass  # 既にラップされているか、閉じられている場合はスキップ
            
            if hasattr(sys.stderr, 'buffer') and not sys.stderr.closed:
                try:
                    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
                except (ValueError, AttributeError):
                    pass  # 既にラップされているか、閉じられている場合はスキップ
    except:
        pass  # エラーが発生しても続行

