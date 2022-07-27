# dirty backup

![https://storage.googleapis.com/dirty-public/logo_main_retina_black.png](https://storage.googleapis.com/dirty-public/logo_main_retina_black.png)

Некогда живое и интересное сообщество d3.ru в последние годы заметно деградировало. А после нападения России на Украину сайт все больше напоминает живой труп.

С помощью этих простых скриптов можно хоть как-то сохранить то, что может быть удалено в любой момент.

Установка зависимостей:

```shell
pip install -r requirements.txt
```

Сохранить в MongoDB посты по идентификатору от 1 до 500000:

```shell
python posts.py 1 500000
```

Посты созраняются в коллекцию `posts`, а комментарии этих постов - в коллекцию `comments`. URL картинок из постов и комментариев сохраняются в коллекцию `media`.
Если при попытке получить пост произошла ошибка, она записывается в коллекцию `failures` вместе с датой и временем, когда эта ошибка произошла.

Обновить посты за последние 90 дней:

```shell
python posts.py 90
```

Обновить все посты:

```shell
python posts.py
```

В последнем случае, в случайном порядке перебираются id постов от максимального id в коллекции `posts` до 0. При этом если пост был обновлен в последние 90 дней, он не обновляется. А ещё не обновляются посты, если их недавно уже пытались получить и произошла ошибка (см. коллекцию `failures`). Но с вероятностью 1/100, данные `failures` игнорируются и данные поста обновляются принудительно.

Индексы MongoDB:

`mongosh --file mongo.js`

Пример поиска по тексту комментариев:

```shell
db.comments.find( { $text: { $search: "jovan" } } )
```

Загрузка картинок в Google Cloud Storage и обновление информации EXIF:

```shell
python media.py
```

### Примеры данных

Самый первый пост:

```shell
db.posts.find().sort({ id: -1 }).limit(1)
```

```JSON
{
    "_id" : "5.1651249358",
    "rating" : 249,
    "domain" : {
        "title" : "",
        "url" : "https://d3.ru",
        "readers_count" : 15,
        "is_subscribed" : false,
        "is_ignored" : false,
        "color_schema" : {
            "irony_color" : "cc3333",
            "links_system_color" : "a1a1a1",
            "background_color" : "ffffff",
            "header_color" : "4e80bd",
            "text_color" : "242424"
        },
        "is_adult" : false,
        "prefix" : "",
        "logo_url" : "https://d3.ru/static/i/logo_main_retina.png",
        "id" : 1
    },
    "views_count" : 17403,
    "unread_comments_count" : 0,
    "country_code" : "",
    "in_favourites" : false,
    "title" : "",
    "data" : {
        "title" : "",
        "text" : "Еще один пост, с другой датой",
        "render_type" : "maxi",
        "snippet" : null,
        "link" : null,
        "media" : null,
        "type" : "link"
    },
    "golden" : true,
    "id" : 5,
    "pinned" : false,
    "user_vote" : null,
    "can_ban" : false,
    "_links" : [ 
        {
            "href" : "https://d3.ru/api/posts/5/report/",
            "params" : null,
            "method" : "post",
            "rel" : "report_post"
        }, 
        {
            "href" : "https://d3.ru/5/",
            "params" : null,
            "method" : "get",
            "rel" : "html"
        }
    ],
    "url_slug" : "",
    "tags" : [],
    "main_image_url" : null,
    "can_moderate" : false,
    "hidden_rating_time_to_show" : null,
    "user" : {
        "is_golden" : false,
        "deleted" : false,
        "gender" : "male",
        "is_ignored" : false,
        "rank" : "",
        "avatar_url" : "https://cdn.jpg.wtf/futurico/50/0b/1419841165-500b2be6558bfcfbf76f898f693b12af.jpg",
        "karma" : 3098,
        "active" : true,
        "login" : "jovan",
        "rank_color" : null,
        "id" : 1
    },
    "can_delete" : false,
    "estimate" : 0,
    "can_unpublish" : false,
    "in_interests" : false,
    "can_edit" : false,
    "favourites_count" : 0,
    "can_change_render_type" : false,
    "created" : 1006434184,
    "changed" : 1651249358,
    "vote_weight" : 1,
    "comments_count" : 553,
    "advertising" : null,
    "has_subscribed" : false,
    "can_comment" : false,
    "url" : "https://d3.ru/5",
    "date" : "2001-11-22",
    "media" : [],
    "fetched" : 1659996492,
    "latest_activity" : 0
}
```

Комментарий с картинкой:

```shell
db.comments.find({ 'media.0': { $exists: true } }).sort({ id: 1 }).limit(1)
```

```JSON
{
    "_id" : "41383.4dbe50a59301f278f0f52c5868840538",
    "body" : "<img src=\"https://cdn.jpg.wtf/futurico/c5/26/1369906240-c526fe58ce5f5097c295ea3711842384.jpeg\" width=\"350\" height=\"278\">",
    "rating" : 0,
    "can_delete" : false,
    "deleted" : false,
    "can_moderate" : false,
    "hidden_rating_time_to_show" : null,
    "tree_level" : 0,
    "country_code" : "",
    "date_order" : 19,
    "id" : 41383,
    "can_edit" : false,
    "created" : 1037110443,
    "vote_weight" : 1,
    "rating_order" : 19,
    "can_remove_comment_threads" : false,
    "user_vote" : null,
    "can_ban" : false,
    "parent_id" : null,
    "unread" : false,
    "user" : {
        "is_golden" : false,
        "deleted" : false,
        "gender" : "male",
        "is_ignored" : false,
        "rank" : "",
        "avatar_url" : "https://cdn.jpg.wtf/futurico/50/0b/1419841165-500b2be6558bfcfbf76f898f693b12af.jpg",
        "karma" : 3098,
        "active" : true,
        "login" : "jovan",
        "rank_color" : null,
        "id" : 1
    },
    "post_id" : 5016,
    "domain" : {
        "title" : "",
        "url" : "https://d3.ru",
        "readers_count" : 15,
        "is_subscribed" : false,
        "is_ignored" : false,
        "color_schema" : {
            "irony_color" : "cc3333",
            "links_system_color" : "a1a1a1",
            "background_color" : "ffffff",
            "header_color" : "4e80bd",
            "text_color" : "242424"
        },
        "is_adult" : false,
        "prefix" : "",
        "logo_url" : "https://d3.ru/static/i/logo_main_retina.png",
        "id" : 1
    },
    "url" : "https://d3.ru/5016#41383",
    "date" : "2002-11-12",
    "media" : [ 
        "https://cdn.jpg.wtf/futurico/c5/26/1369906240-c526fe58ce5f5097c295ea3711842384.jpeg"
    ],
    "fetched" : 1659998583
}
```

Информация о картинке:


```shell
db.media.find({ _id: 'https://cdn.jpg.wtf/futurico/c5/26/1369906240-c526fe58ce5f5097c295ea3711842384.jpeg' })
```

```JSON
{
    "_id" : "https://cdn.jpg.wtf/futurico/c5/26/1369906240-c526fe58ce5f5097c295ea3711842384.jpeg",
    "usage" : [ 
        "https://d3.ru/5016#41383"
    ],
    "hash" : "5750a3644671c7b3917a300701dbe9c46e72b1b95e655b705ac8cec016b0814b",
    "filename" : "1369906240-c526fe58ce5f5097c295ea3711842384.jpeg",
    "content_type" : "image/jpeg",
    "length" : 16110,
    "size" : {
        "width" : 350,
        "height" : 278
    },
    "exif" : {
        "tags" : {},
        "gps" : {},
        "ifd" : {}
    }
}
```

### Количество данных (конец августа 2022):

```shell
db.posts.countDocuments({})
1.193.908
```

```shell
db.comments.countDocuments({})
18.162.403
```

```shell
db.media.countDocuments({})
2.115.785
```