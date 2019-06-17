#encoding=utf8
import csv
import os
import pandas as pd

# 读入所有店铺交易数据
results_list = []
# 从inputfiles里面逐个读出文件信息
for (root, dirs, files) in os.walk('./inputfiles'):
    for file in files:
        filename = os.path.join(root, file)
        print(filename)
        portion = os.path.splitext(file)
        csv_reader = csv.reader(open(filename, encoding='utf-8'))
        count = 0
        # 除了前7行 其他的信息存起来
        for row in csv_reader:
            count += 1
            if count > 8:
                row6 = row[6]
                if row6 == "":
                    row6 = "0"
                results_list.append([portion[0], row[2], row[4], row[5], int(row6.replace(',', '')), \
                                     float(row[12].replace(',', '')), float(row[13].replace(',', '')), \
                                     float(row[14].replace(',', '')), float(row[15].replace(',', '')), \
                                     float(row[16].replace(',', '')), float(row[17].replace(',', '')), \
                                     float(row[18].replace(',', '')), float(row[19].replace(',', '')), \
                                     float(row[20].replace(',', '')), float(row[21].replace(',', '')), \
                                     float(row[22].replace(',', ''))])
# 给results_list添加标题行
all_data = pd.DataFrame(results_list, index=None, columns=['store', 'type', 'sku','description', 'quantity', \
                                                           'product_sales', 'shipping_credits', 'gift_wrap_credits', \
                                                           'promotional_rebates', 'sales_tax_collected', \
                                                           'Marketplace_Facilitator_Tax', 'selling_fees', 'fba_fees', \
                                                           'other_transaction_fees', 'other', 'total'])

df = all_data.copy()

def Select1(codition,title1,title2):
    new_df = df.loc[df.type == codition, ['store', 'type', 'sku', title1]]
    new_df.rename(columns={title1: title2}, inplace=True)
    return new_df

def Select2(title1,title2):
    new_df1 = df.loc[:, ['store', 'type', 'sku', title1]]
    new_df1.rename(columns={title1: title2}, inplace=True)
    return new_df1

# 销售量计算：type==Order,quantity的和
quantity_sales = Select1('Order','quantity','销售量')

# 退款量计算：“Refund”-“product sales”金额为0的数量，（问题：如何减“product sales”金额为0的数量）
quantity_refund = df.loc[(df.type == 'Refund') & (df.product_sales!=0), ['store', 'type', 'sku', 'quantity']]
quantity_refund.rename(columns={'quantity': '退款量'}, inplace=True)

quantity_FBAlose = df.loc[(df.type=='Adjustment') & (df.description!='FBA Inventory Reimbursement - Customer Return') \
                          & (df.description!='FBA Inventory Reimbursement - Damaged:Warehouse'),['store', 'type', 'sku', 'quantity']]
quantity_FBAlose.rename(columns={'quantity': 'FBA丢失量'}, inplace=True)

# 收入类-收到运费
income_shipping_credits = Select2('shipping_credits','运费')

# 收入类-销售收入
income_product_sales = Select2('product_sales','商品价格')

# 收入类-折扣:促销返点
income_promotional_rebates = Select2('promotional_rebates','促销返点')

# 收入类-折扣:销售税
income_sales_tax_collected = Select2('sales_tax_collected','销售税')

# 收入类-折扣:平台服务税
income_Marketplace_Facilitator_Tax = Select2('Marketplace_Facilitator_Tax','平台服务税')

# 店内费用类-佣金
sellingfees = Select2('selling_fees','佣金')

# 店内费用类-亚马逊物流基础服务费
fba = Select2('fba_fees','亚马逊物流基础服务费')

# 店内费用类-广告费用
other_transaction = Select2('other_transaction_fees','广告费用')

# 店内费用类-其他：订单
order_other = Select1('Order','other','其他_订单')

# 店内费用类-其他：服务费
service_fee_other = Select1('Service Fee','other','其他：服务费')

# 店内费用类-其他：其他：退货
refund_other = Select1('Refund','other','其他：退货')

# 店内费用类-其他：其他：清算
adjustment_other = Select1('Adjustment','other','其他：清算')

# 店内费用类-其他：其他：闪电交易费
lightning_Deal_Fee_other = Select1('Lightning Deal Fee','other','其他：闪电交易费')

# 店内费用类-其他：其他：Debt
debt_other = Select1('Debt','other','其他：Debt')

# 店内费用类-其他：其他：退款
chargeback_Refund_other = Select1('Chargeback Refund','other','其他：退款')

# 店内费用类-其他：其他：FBA 库存费
FBA_Inventory_Fee_other = Select1('FBA Inventory Fee','other','其他：FBA 库存费')

# 店内费用类-包装费
gift_wrap_credits_fees = Select2('gift_wrap_credits','包装费')

# 其他：提款
tixian = Select1('Transfer','total', '其他:提款')

# 销售净额
xiaoshoue = df.loc[df.type != 'Transfer', ['store', 'type', 'sku', 'total']]
xiaoshoue.rename(columns={'total': '销售净额'}, inplace=True)

concat_all = pd.concat([quantity_sales, quantity_refund, quantity_FBAlose, income_shipping_credits, \
                        income_product_sales, income_promotional_rebates, income_sales_tax_collected, \
                        income_Marketplace_Facilitator_Tax, sellingfees, fba, other_transaction, \
                        order_other, service_fee_other, refund_other, adjustment_other, \
                        lightning_Deal_Fee_other, debt_other, chargeback_Refund_other, FBA_Inventory_Fee_other, \
                        gift_wrap_credits_fees, xiaoshoue, tixian]).fillna(0)


sales_statistics = concat_all.groupby(['store','sku'])['销售净额','销售量','退款量','FBA丢失量', '运费','商品价格',\
                                                 '促销返点','销售税','平台服务税','佣金','亚马逊物流基础服务费',\
                                                 '广告费用','其他_订单','其他：服务费','其他：退货','其他：清算',\
                                                 '其他：闪电交易费','其他：Debt','其他：退款','其他：FBA 库存费',\
                                                 '包装费','其他:提款'].sum()

sales_statistics=sales_statistics.round(2)

sales_statistics.to_csv('./销售统计1.csv')

print('all done...')
exit(0)
