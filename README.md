## GCS Metadog
GCS-Metadog is a CLI tool for [m3dev/gokart](https://github.com/m3dev/gokart).

Gokart solves reproducibility, task dependencies, constraints of good code, and ease of use for Machine Learning Pipeline.

And, gokart supports GCS as a data store for intermediate results of Tasks in the pipeline.

Then also, gokart is going to support easy searching GCS object from parameter name, task name, dependency of task.

GCS-Metadog is the interface for this purpose.

Thank you for reviewing my code, @kitagry, @hirosassa, @m3dev!

## Installation
Comming soon... (But in a few days, you can install from homebrew, like `brew install gmd`)

## Usage
### Normal Mode
When you want to search GCS object from metadata key only, you can use normal mode search.
You can get full GCS object path list.
```bash
% ./gmd -b ${bucket_name} -m ${metadata_key1} -m {metadata_key} .....
```

### Nest Mode
When you search and filter gokart GCS cache, sometimes, you want to search Task dependency.
For example, you want to search some Tasks those have TaskA(parameter1=1, parameter2=True) as dependency.
You can easily search with gmd by using nest mode.
Parameter and Task names are case sensitive.
```bash
% ./gmd -b ${bucket_name} -t ${dependency_task_name} -p {parameter_name1=parameter_value1} -p {parameter_name1=parameter_value1} .....
```

## Future Work
I plan to support adding multiple sets of -t and -p options in the future, and I will also make installation available via Homebrew.
