import os

IGNORE_DIRS = {
    "__pycache__", "migrations", ".git", ".idea", ".vscode",
    "venv", ".venv", "env", "staticfiles", "media"
}

IGNORE_FILES = {
    ".DS_Store", "db.sqlite3"
}

IGNORE_EXTENSIONS = {".pyc", ".pyo", ".log"}

def generate_tree(startpath, output_file="structure.txt"):
    with open(output_file, "w", encoding="utf-8") as f:
        for root, dirs, files in os.walk(startpath):

            # Filter directories
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]

            # Filter files
            files = [
                file for file in files
                if file not in IGNORE_FILES and not any(file.endswith(ext) for ext in IGNORE_EXTENSIONS)
            ]

            # Write folder
            level = root.replace(startpath, "").count(os.sep)
            indent = " " * 4 * level
            f.write(f"{indent}{os.path.basename(root)}/\n")

            # Write files
            subindent = " " * 4 * (level + 1)
            for file in files:
                f.write(f"{subindent}{file}\n")

generate_tree(os.getcwd())
