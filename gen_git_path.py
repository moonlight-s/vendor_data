import os


def list_all_files(root, skip_dirs=(".git",), skip_exts=(".png",)):
    root = os.path.abspath(root)
    for dirpath, dirnames, filenames in os.walk(root):
        # 原地过滤目录，os.walk 就不会进入这些目录
        dirnames[:] = [d for d in dirnames if d not in skip_dirs]
        for name in filenames:
            if os.path.splitext(name)[1].lower() in skip_exts:
                continue
            yield os.path.join(dirpath, name)


def main():
    root = "C:\\Users\\Sangzi.gao\\Desktop\\gsz\\vendor_data"
    raw = []
    for p in list_all_files(root):
        raw.append(p)

    result = []
    prefix = "https://github.com/moonlight-s/vendor_data/blob/main"
    for p in raw:
        p = p.replace(root, prefix).replace("\\", "/")
        result.append(p)
    return result


if __name__ == "__main__":
    result = main()
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "urls.txt")

    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        for u in result:
            f.write(u + "\n")

    print(f"写了 {len(result)} 行到 {out_path}")
