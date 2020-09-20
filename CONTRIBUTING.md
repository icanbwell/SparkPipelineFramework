# SparkPipelineFramework
Framework for simpler Spark Pipelines


# Publishing package 
For credentials:

Set up your $HOME/.pypirc file like this (replace password with real one):

```
[pypi]
  username = __token__
  password = pypi-AgEIcHlwaS5vcmcCJDU5YTg1ZDZjLTVhOWItNGZmMi1hMTBhLTgzZjVhMzBlYmJhOAACJXsicGVybWlzc2lvbnMiOiAidXNlciIsICJ2ZXJzaW9uIjogMX0AAAYgUAfdyImgcqvyNbLihu22g4Wp_2SYZvvJDx7iYNJpEUg
```

Then:

Increment version in VERSION file

Run ```make package```


# Developer Setup
Run ```make devsetup```

Install following PyCharm plugins:
1. MyPy: https://plugins.jetbrains.com/plugin/11086-mypy
