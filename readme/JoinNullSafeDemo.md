## joinNullSafe() / Join with columns preserving null values

#### Target:
**Have a function to make a simple join with null values also**

#### Explanations:
The default for Spark join are to ignore null values but, sometimes, we need to make some joins even with fields that are null in both sides.
It happen more yet in cases of treatment of data.

#### Parameters:
```scala
  /***
   * Simple join function considering null values
   * @param dfLeft
   *  Source dataframe that will be on the left side of the join
   * @param dfRight
   *  Source dataframe that will be on the right side of the join
   * @param columnsToJoin
   *  Array of columns to join (columns with the same name in both sides)
   * @param joinType
   *  Type of join equal the original join: (inner, left, right, full, ...)
   * @return
   *  Joined dataframe
   */

  def joinNullSafe(dfLeft: DataFrame,
                   dfRight: DataFrame,
                   columnsToJoin: Array[String],
                   joinType: String = "inner") : DataFrame = ???
```
#### Results:
1. Based in the following dataframe in the left side of the join:
```scala
    val dfExample1 = Seq(
      ("A", "01", "Key A01"),
      ("A", "02", "Key A02"),
      ("B", "01", "Key B01"),
      ("B", null, "Key B"),
      ("C", "01", "Key C01")
    ).toDF("key_letter", "digit", "key_description")
/*
+----------+-----+---------------+
|key_letter|digit|key_description|
+----------+-----+---------------+
|         A|   01|        Key A01|
|         A|   02|        Key A02|
|         B|   01|        Key B01|
|         B| null|          Key B|
|         C|   01|        Key C01|
+----------+-----+---------------+
*/
```

2. And the following dataframe in the right side of the join:
```scala
    val dfExample2 = Seq(
      ("A", "01", 1, "Item A01.1"),
      ("A", "01", 2, "Item A01.2"),
      ("A", "02", 1, "Item A02.1"),
      ("B", "01", 1, "Item B01.1"),
      ("B", "01", 2, "Item B01.2"),
      ("B", null, 1, "Item B.1"),
      ("B", null, 2, "Item B.2"),
      ("D", "01", 1, "Item D01.1")
    ).toDF("key_letter", "digit", "item", "item_description")
/*    
+----------+-----+----+----------------+
|key_letter|digit|item|item_description|
+----------+-----+----+----------------+
|         A|   01|   1|      Item A01.1|
|         A|   01|   2|      Item A01.2|
|         A|   02|   1|      Item A02.1|
|         B|   01|   1|      Item B01.1|
|         B|   01|   2|      Item B01.2|
|         B| null|   1|        Item B.1|
|         B| null|   2|        Item B.2|
|         D|   01|   1|      Item D01.1|
+----------+-----+----+----------------+
*/
```
3. With the columns to join in a Array/List (the same name in both sides):
```scala
    // Columns defined for the join
    val columnsToJoin = Array("key_letter", "digit")
```
4. The results for a inner join (without and with nulls) are:

**Without null**:
```
+----------+-----+---------------+----+----------------+
|key_letter|digit|key_description|item|item_description|
+----------+-----+---------------+----+----------------+
|         A|   01|        Key A01|   1|      Item A01.1|
|         A|   01|        Key A01|   2|      Item A01.2|
|         A|   02|        Key A02|   1|      Item A02.1|
|         B|   01|        Key B01|   1|      Item B01.1|
|         B|   01|        Key B01|   2|      Item B01.2|
+----------+-----+---------------+----+----------------+
```
**With null safe**
```
+----------+-----+---------------+----+----------------+
|key_letter|digit|key_description|item|item_description|
+----------+-----+---------------+----+----------------+
|         A|   01|        Key A01|   1|      Item A01.1|
|         A|   01|        Key A01|   2|      Item A01.2|
|         A|   02|        Key A02|   1|      Item A02.1|
|         B|   01|        Key B01|   1|      Item B01.1|
|         B|   01|        Key B01|   2|      Item B01.2|
|         B| null|          Key B|   1|        Item B.1|
|         B| null|          Key B|   2|        Item B.2|
+----------+-----+---------------+----+----------------+
```
*PS: It's an example, there is a demo file to test other possibility*