## pykit

### introduction
This repository contains different tools that may be useful for automation of tasks.
Below is a short list of some of the tools available in this repository.

#### Sweeper
Allows the user to sweep through different values in a nested dictionary:
```python
from pykit.sweeper import DictSweep, StrategyName

data = {
    'a': [1,2],
    'b': [3,4]
}

my_sweeper = DictSweep(parameters=data, strategy=StrategyName.PRODUCT)
for elem in my_sweeper.generate():
    print(elem) 

# Output: {'a' : 1, 'b': 3}
# Output: {'a' : 1, 'b': 4}
# Output: {'a' : 2, 'b': 3}
# Output: {'a' : 2, 'b': 4}
```

#### Project
Helps with creating folders and saving files in a structured way:
```python

from pykit.project import Project

my_project = Project(root='my_main_folder')

# let's say I would like to create a subfolder called 'data'
my_data_folder = my_project.group('data')

# next I would like to generate a unique name for my spectroscopy experiment data
# using .start(identifier) I will get a unique name based on the identifier and also tagged its creation time
unique_name_to_save_data = my_data_folder.start(identifier='spectroscopy_experiment')

# now I finished the experiment and i would like to signal it is done and 
# also to attach some extra files that are related to this experiment
my_data_folder.finish(identifier='spectroscopy_experiment', base=unique_name_to_save_data,
                      path_dict={'extra_information': 'path_to_extra_information.txt'})

```

