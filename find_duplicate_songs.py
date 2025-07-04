import os
import hashlib
from multiprocessing import Pool, cpu_count
from collections import defaultdict

# U盘路径
USB_PATH = '/Volumes/DISK_IMG'

# 支持的音乐格式
MUSIC_EXTENSIONS = {'.mp3', '.flac', '.wav', '.aac', '.m4a', '.ogg'}

def is_music_file(filename):
    return any(filename.lower().endswith(ext) for ext in MUSIC_EXTENSIONS)

def get_all_music_files(directory):
    """获取所有音乐文件及其大小"""
    music_files = []
    for root, _, files in os.walk(directory):
        for filename in files:
            if is_music_file(filename):
                filepath = os.path.join(root, filename)
                try:
                    size = os.path.getsize(filepath)
                    music_files.append((filepath, size))
                except Exception as e:
                    print(f"⚠️ 无法访问文件 {filepath}: {e}")
    return music_files

def compute_file_hash(filepath):
    """计算文件的 SHA-256"""
    sha256 = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return (filepath, sha256.hexdigest())
    except Exception as e:
        print(f"⚠️ 读取失败: {filepath}: {e}")
        return (filepath, None)

def group_by_size(files_with_size):
    """将文件按大小分组"""
    size_groups = defaultdict(list)
    for filepath, size in files_with_size:
        size_groups[size].append(filepath)
    return {size: paths for size, paths in size_groups.items() if len(paths) > 1}

def find_duplicates_by_hash(size_grouped_files):
    """对每个大小组内的文件进行哈希比对"""
    duplicates = defaultdict(list)
    for size, filepaths in size_grouped_files.items():
        print(f"📦 大小为 {size} 字节的文件组，共 {len(filepaths)} 个，正在计算哈希…")
        with Pool(processes=cpu_count()) as pool:
            results = pool.map(compute_file_hash, filepaths)
        hash_map = defaultdict(list)
        for filepath, file_hash in results:
            if file_hash:
                hash_map[file_hash].append(filepath)
        for file_hash, paths in hash_map.items():
            if len(paths) > 1:
                duplicates[file_hash].extend(paths)
    return duplicates

def print_duplicates(duplicates):
    print(f"\n🔍 找到 {len(duplicates)} 组重复文件：\n")
    for i, (file_hash, paths) in enumerate(duplicates.items(), 1):
        print(f"🔁 重复组 #{i}:")
        for path in paths:
            print(f"  - {path}")
        print()

if __name__ == '__main__':
    print(f"📂 扫描目录: {USB_PATH}")
    music_files = get_all_music_files(USB_PATH)
    print(f"🎶 总共找到音乐文件: {len(music_files)} 个")

    # 按文件大小分组
    grouped_by_size = group_by_size(music_files)
    print(f"🔎 发现 {len(grouped_by_size)} 个大小相同的文件组，准备进一步哈希比对")

    # 对大小相同的组进行哈希对比
    duplicates = find_duplicates_by_hash(grouped_by_size)

    # 输出结果
    print_duplicates(duplicates)