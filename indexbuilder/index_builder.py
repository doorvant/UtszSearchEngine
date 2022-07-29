from whoosh.fields import Schema, ID, TEXT, NUMERIC
from whoosh.index import create_in, open_dir
from jieba.analyse import ChineseAnalyzer
import pymongo
from pymongo.collection import Collection
import settings


class IndexBuilder:
    def __init__(self):
        self.mongoClient = pymongo.MongoClient(host=settings.MONGODB_HOST, port=settings.MONGODB_PORT)
        self.db = pymongo.database.Database(self.mongoClient, settings.MONGODB_DBNAME)
        self.pagesCollection = Collection(self.db, settings.MONGODB_SHEETNAME)

    def build_index(self):
        analyzer = ChineseAnalyzer()

        # 索引模板
        schema = Schema(
            newsId=ID(stored=True),
            newsTitle=TEXT(stored=True, analyzer=analyzer),
            newsUrl=ID(stored=True),
            newsPublishTime=TEXT(stored=True),
            newsContent=TEXT(stored=False, analyzer=analyzer),
        )

        import os.path
        if not os.path.exists('index'):
            os.mkdir('index')
            ix = create_in('index', schema)
            print('未发现索引文件')
        else:
            ix = open_dir('index')
            print('发现索引文件并开始载入')

        # 构建
        writer = ix.writer()
        indexed_amount = 0
        total_amount = self.pagesCollection.count_documents({})
        false_amount = self.pagesCollection.count_documents({'indexed': 'False'})
        print(false_amount, '/', total_amount)
        while True:
            try:
                row = self.pagesCollection.find_one({'indexed': 'False'})
                if row is None:
                    writer.commit()
                    print('所有索引构建完毕.')
                    break
                else:
                    writer.add_document(
                        newsId=str(row['_id']),
                        newsTitle=row['newsTitle'],
                        newsUrl=row['newsUrl'],
                        newsPublishTime=row['newsPublishTime'],
                        newsContent=row['newsContent'],
                    )

                    # the end
                    self.pagesCollection.update_one({'_id': row['_id']}, {'$set': {'indexed': 'True'}})
                    writer.commit()         # 每次构建提交一次
                    writer = ix.writer()    # 然后再重新打开
                    indexed_amount += 1
                    print(indexed_amount, '/', false_amount, '/', total_amount)

            except:
                print(row['_id'], '异常.')
                print('已处理', indexed_amount, '/', total_amount, '项.')
                break


if __name__ == '__main__':
    id = IndexBuilder()
    id.build_index()
