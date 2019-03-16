dataset.groupBy("group_column")
.count
.sort($"count".desc)
.show(5)
