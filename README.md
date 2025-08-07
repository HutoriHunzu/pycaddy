# pycaddy

A Python toolbox caddy for experiment tracking, parameter sweeping, and automation tasks.

## Installation

```bash
pip install pycaddy
```

## Quick Start

### Experiment Tracking

```python
from pycaddy.project import Project

# Create a project for organizing experiments  
project = Project(root="experiments").ensure_folder()

# Start a new experiment run
session = project.session("train", params={"lr": 0.001, "batch_size": 32})
session.start()

# Your experiment code here...
model_path = session.path("model.pt")
# save_model(model_path)

# Mark as completed
session.done()
```

### Parameter Sweeping

```python
from pycaddy.sweeper import DictSweep, StrategyName

# Define parameter space
params = {
    'learning_rate': [0.01, 0.001],
    'batch_size': [16, 32, 64]
}

# Generate all combinations
sweep = DictSweep(parameters=params, strategy=StrategyName.PRODUCT)
for config in sweep.generate():
    print(config)
    # {'learning_rate': 0.01, 'batch_size': 16}
    # {'learning_rate': 0.01, 'batch_size': 32}
    # ... etc
```

## Features

- **Project Management**: Structured folder organization with automatic metadata tracking
- **Session Tracking**: Track experiment runs with unique IDs, status, and file attachments  
- **Parameter Sweeping**: Generate parameter combinations with different strategies
- **Concurrent Safe**: File-based locking for multi-process experiment tracking
- **Lightweight**: Minimal dependencies, designed as a dependency toolbox

## License

MIT License - see [LICENSE](LICENSE) file.

## Links

- **Repository**: https://github.com/HutoriHunzu/pycaddy
- **Author**: Uri Goldblatt (uri.goldblatt@gmail.com)