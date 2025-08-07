from matplotlib.figure import Figure
import matplotlib.pyplot as plt
from pathlib import Path


def save_fig(path: Path | str,
             fig: Figure,
             close: bool = True,
             add_suffix: bool = True,
             dpi: int = 300):
    """Save a matplotlib figure to a file.

    Args:
        :param fig: The matplotlib figure object to save.
        :param path: The file path where the figure will be saved.
        :param dpi: The resolution in dots per inch for the saved figure. Defaults to 300.
        :param close: Whether to close the figure after saving. Defaults to True.
        :param add_suffix: Whether to add a '.png' suffix to the file name if not already present. Defaults to True.
    """
    path = Path(path)
    if add_suffix:
        path = path.with_suffix('.png')

    fig.savefig(path, dpi=dpi, bbox_inches='tight')
    if close:
        plt.close(fig)  # Close the figure to free up memory

    return path
