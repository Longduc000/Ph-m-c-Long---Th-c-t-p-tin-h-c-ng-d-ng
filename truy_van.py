import pyspark.sql.functions as F
import time
from pyspark.sql import SparkSession

# Tạo Spark session
spark = SparkSession.builder.appName("SparkSQLDemo").getOrCreate()

# Đọc dữ liệu từ file csv vào DataFrame
df = spark.read.csv("1_trieu_hang.csv", header=True)

# **Thao tác CRUD cơ bản:**

### Tạo (Create)
print("create")
# Thêm một cột mới `cot4` với giá trị ngẫu nhiên cho mỗi hàng
print("Thêm một cột mới `cot4` với giá trị ngẫu nhiên cho mỗi hàng")
df = df.withColumn("cot4", F.rand())
df.show()
### Đọc (Read)
print("read")
# Xem toàn bộ dữ liệu
print("20 dòng đầu tiên")
df.show()
# Xem 10 hàng đầu tiên
print("10 hàng đầu tiên")
df.show(10)
# Lọc dữ liệu theo điều kiện
df.filter(df["cot1"] > 500).show()

###  Cập nhật (Update)
print("update")
# Lọc các hàng có cot1 lớn hơn 700
print("Lọc các hàng có cot1 lớn hơn 700")
filtered_df = df.filter(df["cot1"] > 700)
# Cập nhật cột "cot2" với giá trị cố định (1000) cho các hàng được lọc
print("Cập nhật cột cot2 với giá trị cố định (1000) cho các hàng được lọc")
updated_df = filtered_df.withColumn("cot2", F.lit(1000))
df.show()

### xoá (delete)
print("delete")
# Lọc các hàng có cot3 nhỏ hơn 300
print("Lọc các hàng có cot3 nhỏ hơn 300")
# Giữ các hàng có cot3 >= 300, bỏ các hàng đã lọc (tức là các hàng có cot3 < 300) khỏi DataFrame gốc
print("Giữ các hàng có cot3 >= 300, bỏ các hàng đã lọc (tức là các hàng có cot3 < 300) khỏi DataFrame gốc")
filtered_df = df.filter(df["cot3"] >= 300)  
df = filtered_df.drop() 
df.show()

print("thời gia chạy của một số truy vấn")
# Without WHERE clause (consider using for initial exploration)
start_time_no_where = time.time()
df.filter(F.lit(True)).show()  # Filter with a constant true expression
end_time_no_where = time.time()
print(f"Thời gian không có WHERE: {end_time_no_where - start_time_no_where:.2f} seconds")

# With WHERE clause (more efficient for filtering)
start_time_with_where = time.time()
filtered_df = df.filter(df["cot1"] > 500)
filtered_df.show()
end_time_with_where = time.time()
print(f"Thời gian có WHERE (cot1 > 500): {end_time_with_where - start_time_with_where:.2f} seconds")