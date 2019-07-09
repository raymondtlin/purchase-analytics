from pathlib import Path

def get_project_root() -> Path:
    """
    Helper to get project root as Path.
    :return: Path object for project root
     """
    root = Path(__file__).parent.parent
    return root

