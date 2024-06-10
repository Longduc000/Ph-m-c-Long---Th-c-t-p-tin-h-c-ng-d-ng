import csv
import random

# Mở file csv để ghi
with open('1_trieu_hang.csv', 'w', newline='') as csvfile:
  # Tạo object writer
  writer = csv.writer(csvfile)
  # Viết tiêu đề cột
  writer.writerow(['cot1', 'cot2', 'cot3'])

  # Tạo 1 triệu hàng dữ liệu ngẫu nhiên
  for i in range(1000000):
    cot1 = random.randint(1, 1000)
    cot2 = random.randint(1, 1000)
    cot3 = random.randint(1, 1000)
    # Ghi hàng dữ liệu
    writer.writerow([cot1, cot2, cot3])