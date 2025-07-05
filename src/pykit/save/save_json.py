import json
import numpy as np
from pathlib import Path
from dataclasses import is_dataclass, asdict
from typing import Any


class JsonEncoder(json.JSONEncoder):
    def default(self, obj: Any):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            if np.iscomplexobj(obj):
                return {'real part': np.real(obj).tolist(), 'imaginary part': np.imag(obj).tolist()}
            else:
                return obj.tolist()
        elif isinstance(obj, complex):
            return {'real part': obj.real, 'imaginary part': obj.imag}
        elif is_dataclass(obj):
            return asdict(obj)
        elif isinstance(obj, (tuple, set)):
            return list(obj)
        # elif isinstance(obj, ModelResult):
        #     return {param_name: {'value': param.value, 'stderr': param.stderr}
        #             for param_name, param in obj.params.items()}
        else:
            print(f"Couldn't serialize {type(obj)}. Solve it or save it as `pickle`.")
            return super(JsonEncoder, self).default(obj)


def save_json(path: Path | str, data: dict, add_suffix: bool = True):
    d = json.dumps(data, cls=JsonEncoder, indent=2)
    path = Path(path)
    if add_suffix:
        path = path.with_suffix('.json')

    with open(path, 'w') as f:
        f.write(d)

    return path
