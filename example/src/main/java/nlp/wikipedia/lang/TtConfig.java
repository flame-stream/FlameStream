/**
 * This file is part of Wikiforia.
 *
 * Wikiforia is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * Wikiforia is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Wikiforia. If not, see <http://www.gnu.org/licenses/>.
 */
 package nlp.wikipedia.lang;

//Autogenerated from Wikimedia sources at 2015-04-16T13:55:11+00:00

public class TtConfig extends TemplateConfig {
	public TtConfig() {
		addNamespaceAlias(-2, "Медиа", "Медиа");
		addNamespaceAlias(-1, "Махсус", "Служебная", "Maxsus", "Служебная");
		addNamespaceAlias(1, "Бәхәс", "Обсуждение", "Фикер алышу", "Bäxäs", "Обсуждение");
		addNamespaceAlias(2, "Кулланучы", "Участница", "Участник", "Äğzä", "Участник", "Участник", "Участница");
		addNamespaceAlias(3, "Кулланучы_бәхәсе", "Обсуждение участницы", "Обсуждение_участника", "Äğzä_bäxäse", "Обсуждение_участника", "Обсуждение_участника", "Обсуждение_участницы");
		addNamespaceAlias(5, "Wikipedia_бәхәсе", "Обсуждение_{{GRAMMAR:genitive|Wikipedia}}", "Wikipedia_bäxäse", "Обсуждение_{{GRAMMAR:genitive|Wikipedia}}");
		addNamespaceAlias(6, "Файл", "Изображение", "Рәсем", "Räsem", "Файл", "Изображение");
		addNamespaceAlias(7, "Файл_бәхәсе", "Обсуждение_изображения", "Обсуждение_файла", "Рәсем_бәхәсе", "Räsem_bäxäse", "Обсуждение_файла", "Обсуждение_изображения");
		addNamespaceAlias(8, "МедиаВики", "Медиа_Вики", "MediaWiki");
		addNamespaceAlias(9, "МедиаВики_бәхәсе", "Обсуждение_MediaWiki", "Медиа_Вики_бәхәсе", "MediaWiki_bäxäse", "Обсуждение_MediaWiki");
		addNamespaceAlias(10, "Калып", "Үрнәк", "Шаблон", "Ürnäk", "Шаблон");
		addNamespaceAlias(11, "Калып_бәхәсе", "Үрнәк_бәхәсе", "Обсуждение_шаблона", "Шаблон_бәхәсе", "Ürnäk_bäxäse", "Обсуждение_шаблона");
		addNamespaceAlias(12, "Ярдәм", "Справка", "Yärdäm", "Справка");
		addNamespaceAlias(13, "Ярдәм_бәхәсе", "Обсуждение_справки", "Yärdäm_bäxäse", "Обсуждение_справки");
		addNamespaceAlias(14, "Төркем", "Категория", "Törkem", "Категория");
		addNamespaceAlias(15, "Төркем_бәхәсе", "Обсуждение_категории", "Törkem_bäxäse", "Обсуждение_категории");

		addI18nCIAlias("redirect", "#ЮНӘЛТҮ", "#перенаправление", "#перенапр", "#REDIRECT", "Array");
		addI18nCIAlias("notoc", "__БАШЛЫКЮК__", "__БЕЗ_ОГЛАВЛЕНИЯ__", "__БЕЗ_ОГЛ__", "__NOTOC__", "Array");
		addI18nCIAlias("forcetoc", "__ETTIQ__", "__ОБЯЗ_ОГЛ__", "__ОБЯЗАТЕЛЬНОЕ_ОГЛАВЛЕНИЕ__", "__FORCETOC__", "Array");
		addI18nCIAlias("toc", "__ЭЧТЕЛЕК__", "__ОГЛАВЛЕНИЕ__", "__ОГЛ__", "__TOC__", "Array");
		addI18nCIAlias("noeditsection", "__БҮЛЕКҮЗГӘРТҮЮК__", "__БЕЗ_РЕДАКТИРОВАНИЯ_РАЗДЕЛА__", "__NOEDITSECTION__", "Array");
		addI18nAlias("currentmonth", "АГЫМДАГЫ_АЙ", "АГЫМДАГЫ_АЙ2", "ТЕКУЩИЙ_МЕСЯЦ", "ТЕКУЩИЙ_МЕСЯЦ_2", "CURRENTMONTH", "CURRENTMONTH2", "Array");
		addI18nAlias("currentmonthname", "АГЫМДАГЫ_АЙ_ИСЕМЕ", "НАЗВАНИЕ_ТЕКУЩЕГО_МЕСЯЦА", "CURRENTMONTHNAME", "Array");
		addI18nAlias("currentmonthnamegen", "АГЫМДАГЫ_АЙ_ИСЕМЕ_GEN", "НАЗВАНИЕ_ТЕКУЩЕГО_МЕСЯЦА_РОД", "CURRENTMONTHNAMEGEN", "Array");
		addI18nAlias("currentday", "АГЫМДАГЫ_КӨН", "ТЕКУЩИЙ_ДЕНЬ", "CURRENTDAY", "Array");
		addI18nAlias("currentday2", "АГЫМДАГЫ_КӨН2", "ТЕКУЩИЙ_ДЕНЬ_2", "CURRENTDAY2", "Array");
		addI18nAlias("currentdayname", "АГЫМДАГЫ_КӨН_ИСЕМЕ", "НАЗВАНИЕ_ТЕКУЩЕГО_ДНЯ", "CURRENTDAYNAME", "Array");
		addI18nAlias("currentyear", "АГЫМДАГЫ_ЕЛ", "ТЕКУЩИЙ_ГОД", "CURRENTYEAR", "Array");
		addI18nAlias("currenttime", "АГЫМДАГЫ_ВАКЫТ", "ТЕКУЩЕЕ_ВРЕМЯ", "CURRENTTIME", "Array");
		addI18nAlias("numberofarticles", "МӘКАЛӘ_САНЫ", "КОЛИЧЕСТВО_СТАТЕЙ", "NUMBEROFARTICLES", "Array");
		addI18nAlias("pagename", "БИТ_ИСЕМЕ", "НАЗВАНИЕ_СТРАНИЦЫ", "PAGENAME", "Array");
		addI18nAlias("namespace", "ИСЕМНӘР_МӘЙДАНЫ", "ПРОСТРАНСТВО_ИМЁН", "NAMESPACE", "Array");
		addI18nCIAlias("msg", "ХӘБӘР", "СООБЩЕНИЕ:", "СООБЩ:", "MSG:", "Array");
		addI18nCIAlias("subst", "TÖPÇEK:", "ПОДСТ:", "ПОДСТАНОВКА:", "SUBST:", "Array");
		addI18nAlias("img_right", "уңда", "справа", "right", "Array");
		addI18nAlias("img_left", "сулда", "слева", "left", "Array");
		addI18nAlias("img_none", "юк", "без", "none", "Array");
		addI18nAlias("img_width", "$1пкс", "$1px", "Array");
		addI18nAlias("img_center", "үзәк", "центр", "center", "centre", "Array");
		addI18nCIAlias("int", "ЭЧКЕ:", "ВНУТР:", "INT:", "Array");
		addI18nAlias("sitename", "СӘХИФӘ_ИСЕМЕ", "НАЗВАНИЕ_САЙТА", "SITENAME", "Array");
		addI18nCIAlias("ns", "İA:", "ПИ:", "NS:", "Array");
		addI18nCIAlias("localurl", "URINLIURL:", "ЛОКАЛЬНЫЙ_АДРЕС:", "LOCALURL:", "Array");
		addI18nCIAlias("localurle", "URINLIURLE:", "ЛОКАЛЬНЫЙ_АДРЕС_2:", "LOCALURLE:", "Array");
		addI18nCIAlias("language", "#ТЕЛ:", "#ЯЗЫК:", "#LANGUAGE:", "Array");
		addI18nCIAlias("special", "махсус", "служебная", "special", "Array");
		addI18nCIAlias("tag", "тамга", "метка", "тег", "тэг", "tag", "Array");
		addI18nAlias("noindex", "__ИНДЕКССЫЗ__", "__БЕЗ_ИНДЕКСА__", "__NOINDEX__", "Array");
		addI18nCIAlias("nogallery", "__БЕЗ_ГАЛЕРЕИ__", "__NOGALLERY__");
		addI18nAlias("currentmonth1", "ТЕКУЩИЙ_МЕСЯЦ_1", "CURRENTMONTH1");
		addI18nAlias("currentmonthabbrev", "НАЗВАНИЕ_ТЕКУЩЕГО_МЕСЯЦА_АБР", "CURRENTMONTHABBREV");
		addI18nAlias("currenthour", "ТЕКУЩИЙ_ЧАС", "CURRENTHOUR");
		addI18nAlias("localmonth", "МЕСТНЫЙ_МЕСЯЦ", "МЕСТНЫЙ_МЕСЯЦ_2", "LOCALMONTH", "LOCALMONTH2");
		addI18nAlias("localmonth1", "МЕСТНЫЙ_МЕСЯЦ_1", "LOCALMONTH1");
		addI18nAlias("localmonthname", "НАЗВАНИЕ_МЕСТНОГО_МЕСЯЦА", "LOCALMONTHNAME");
		addI18nAlias("localmonthnamegen", "НАЗВАНИЕ_МЕСТНОГО_МЕСЯЦА_РОД", "LOCALMONTHNAMEGEN");
		addI18nAlias("localmonthabbrev", "НАЗВАНИЕ_МЕСТНОГО_МЕСЯЦА_АБР", "LOCALMONTHABBREV");
		addI18nAlias("localday", "МЕСТНЫЙ_ДЕНЬ", "LOCALDAY");
		addI18nAlias("localday2", "МЕСТНЫЙ_ДЕНЬ_2", "LOCALDAY2");
		addI18nAlias("localdayname", "НАЗВАНИЕ_МЕСТНОГО_ДНЯ", "LOCALDAYNAME");
		addI18nAlias("localyear", "МЕСТНЫЙ_ГОД", "LOCALYEAR");
		addI18nAlias("localtime", "МЕСТНОЕ_ВРЕМЯ", "LOCALTIME");
		addI18nAlias("localhour", "МЕСТНЫЙ_ЧАС", "LOCALHOUR");
		addI18nAlias("numberofpages", "КОЛИЧЕСТВО_СТРАНИЦ", "NUMBEROFPAGES");
		addI18nAlias("numberoffiles", "КОЛИЧЕСТВО_ФАЙЛОВ", "NUMBEROFFILES");
		addI18nAlias("numberofusers", "КОЛИЧЕСТВО_УЧАСТНИКОВ", "NUMBEROFUSERS");
		addI18nAlias("numberofactiveusers", "КОЛИЧЕСТВО_АКТИВНЫХ_УЧАСТНИКОВ", "NUMBEROFACTIVEUSERS");
		addI18nAlias("numberofedits", "КОЛИЧЕСТВО_ПРАВОК", "NUMBEROFEDITS");
		addI18nAlias("numberofviews", "КОЛИЧЕСТВО_ПРОСМОТРОВ", "NUMBEROFVIEWS");
		addI18nAlias("pagenamee", "НАЗВАНИЕ_СТРАНИЦЫ_2", "PAGENAMEE");
		addI18nAlias("namespacee", "ПРОСТРАНСТВО_ИМЁН_2", "NAMESPACEE");
		addI18nAlias("namespacenumber", "НОМЕР_ПРОСТРАНСТВА_ИМЁН", "NAMESPACENUMBER");
		addI18nAlias("talkspace", "ПРОСТРАНСТВО_ОБСУЖДЕНИЙ", "TALKSPACE");
		addI18nAlias("talkspacee", "ПРОСТРАНСТВО_ОБСУЖДЕНИЙ_2", "TALKSPACEE");
		addI18nAlias("subjectspace", "ПРОСТРАНСТВО_СТАТЕЙ", "SUBJECTSPACE", "ARTICLESPACE");
		addI18nAlias("subjectspacee", "ПРОСТРАНСТВО_СТАТЕЙ_2", "SUBJECTSPACEE", "ARTICLESPACEE");
		addI18nAlias("fullpagename", "ПОЛНОЕ_НАЗВАНИЕ_СТРАНИЦЫ", "FULLPAGENAME");
		addI18nAlias("fullpagenamee", "ПОЛНОЕ_НАЗВАНИЕ_СТРАНИЦЫ_2", "FULLPAGENAMEE");
		addI18nAlias("subpagename", "НАЗВАНИЕ_ПОДСТРАНИЦЫ", "SUBPAGENAME");
		addI18nAlias("subpagenamee", "НАЗВАНИЕ_ПОДСТРАНИЦЫ_2", "SUBPAGENAMEE");
		addI18nAlias("basepagename", "ОСНОВА_НАЗВАНИЯ_СТРАНИЦЫ", "BASEPAGENAME");
		addI18nAlias("basepagenamee", "ОСНОВА_НАЗВАНИЯ_СТРАНИЦЫ_2", "BASEPAGENAMEE");
		addI18nAlias("talkpagename", "НАЗВАНИЕ_СТРАНИЦЫ_ОБСУЖДЕНИЯ", "TALKPAGENAME");
		addI18nAlias("talkpagenamee", "НАЗВАНИЕ_СТРАНИЦЫ_ОБСУЖДЕНИЯ_2", "TALKPAGENAMEE");
		addI18nAlias("subjectpagename", "НАЗВАНИЕ_СТРАНИЦЫ_СТАТЬИ", "SUBJECTPAGENAME", "ARTICLEPAGENAME");
		addI18nAlias("subjectpagenamee", "НАЗВАНИЕ_СТРАНИЦЫ_СТАТЬИ_2", "SUBJECTPAGENAMEE", "ARTICLEPAGENAMEE");
		addI18nCIAlias("safesubst", "ЗАЩПОДСТ:", "SAFESUBST:");
		addI18nCIAlias("msgnw", "СООБЩ_БЕЗ_ВИКИ:", "MSGNW:");
		addI18nAlias("img_thumbnail", "мини", "миниатюра", "thumbnail", "thumb");
		addI18nAlias("img_manualthumb", "мини=$1", "миниатюра=$1", "thumbnail=$1", "thumb=$1");
		addI18nAlias("img_framed", "обрамить", "framed", "enframed", "frame");
		addI18nAlias("img_frameless", "безрамки", "frameless");
		addI18nAlias("img_page", "страница=$1", "страница $1", "page=$1", "page $1");
		addI18nAlias("img_upright", "сверхусправа", "сверхусправа=$1", "сверхусправа $1", "upright", "upright=$1", "upright $1");
		addI18nAlias("img_border", "граница", "border");
		addI18nAlias("img_baseline", "основание", "baseline");
		addI18nAlias("img_sub", "под", "sub");
		addI18nAlias("img_super", "над", "super", "sup");
		addI18nAlias("img_top", "сверху", "top");
		addI18nAlias("img_text_top", "текст-сверху", "text-top");
		addI18nAlias("img_middle", "посередине", "middle");
		addI18nAlias("img_bottom", "снизу", "bottom");
		addI18nAlias("img_text_bottom", "текст-снизу", "text-bottom");
		addI18nAlias("img_link", "ссылка=$1", "link=$1");
		addI18nAlias("img_alt", "альт=$1", "alt=$1");
		addI18nCIAlias("nse", "ПИК:", "NSE:");
		addI18nCIAlias("articlepath", "ПУТЬ_К_СТАТЬЕ", "ARTICLEPATH");
		addI18nCIAlias("pageid", "ИДЕНТИФИКАТОР_СТРАНИЦЫ", "PAGEID");
		addI18nCIAlias("server", "СЕРВЕР", "SERVER");
		addI18nCIAlias("servername", "НАЗВАНИЕ_СЕРВЕРА", "SERVERNAME");
		addI18nCIAlias("scriptpath", "ПУТЬ_К_СКРИПТУ", "SCRIPTPATH");
		addI18nCIAlias("stylepath", "ПУТЬ_К_СТИЛЮ", "STYLEPATH");
		addI18nCIAlias("grammar", "ПАДЕЖ:", "GRAMMAR:");
		addI18nCIAlias("gender", "ПОЛ:", "GENDER:");
		addI18nCIAlias("notitleconvert", "__БЕЗ_ПРЕОБРАЗОВАНИЯ_ЗАГОЛОВКА__", "__NOTITLECONVERT__", "__NOTC__");
		addI18nCIAlias("nocontentconvert", "__БЕЗ_ПРЕОБРАЗОВАНИЯ_ТЕКСТА__", "__NOCONTENTCONVERT__", "__NOCC__");
		addI18nAlias("currentweek", "ТЕКУЩАЯ_НЕДЕЛЯ", "CURRENTWEEK");
		addI18nAlias("currentdow", "ТЕКУЩИЙ_ДЕНЬ_НЕДЕЛИ", "CURRENTDOW");
		addI18nAlias("localweek", "МЕСТНАЯ_НЕДЕЛЯ", "LOCALWEEK");
		addI18nAlias("localdow", "МЕСТНЫЙ_ДЕНЬ_НЕДЕЛИ", "LOCALDOW");
		addI18nAlias("revisionid", "ИД_ВЕРСИИ", "REVISIONID");
		addI18nAlias("revisionday", "ДЕНЬ_ВЕРСИИ", "REVISIONDAY");
		addI18nAlias("revisionday2", "ДЕНЬ_ВЕРСИИ_2", "REVISIONDAY2");
		addI18nAlias("revisionmonth", "МЕСЯЦ_ВЕРСИИ", "REVISIONMONTH");
		addI18nAlias("revisionmonth1", "МЕСЯЦ_ВЕРСИИ_1", "REVISIONMONTH1");
		addI18nAlias("revisionyear", "ГОД_ВЕРСИИ", "REVISIONYEAR");
		addI18nAlias("revisiontimestamp", "ОТМЕТКА_ВРЕМЕНИ_ВЕРСИИ", "REVISIONTIMESTAMP");
		addI18nAlias("revisionuser", "ВЕРСИЯ_УЧАСТНИКА", "REVISIONUSER");
		addI18nCIAlias("plural", "МНОЖЕСТВЕННОЕ_ЧИСЛО:", "PLURAL:");
		addI18nCIAlias("fullurl", "ПОЛНЫЙ_АДРЕС:", "FULLURL:");
		addI18nCIAlias("fullurle", "ПОЛНЫЙ_АДРЕС_2:", "FULLURLE:");
		addI18nCIAlias("lcfirst", "ПЕРВАЯ_БУКВА_МАЛЕНЬКАЯ:", "LCFIRST:");
		addI18nCIAlias("ucfirst", "ПЕРВАЯ_БУКВА_БОЛЬШАЯ:", "UCFIRST:");
		addI18nCIAlias("lc", "МАЛЕНЬКИМИ_БУКВАМИ:", "LC:");
		addI18nCIAlias("uc", "БОЛЬШИМИ_БУКВАМИ:", "UC:");
		addI18nCIAlias("raw", "НЕОБРАБ:", "RAW:");
		addI18nAlias("displaytitle", "ПОКАЗАТЬ_ЗАГОЛОВОК", "DISPLAYTITLE");
		addI18nAlias("rawsuffix", "Н", "R");
		addI18nAlias("newsectionlink", "__ССЫЛКА_НА_НОВЫЙ_РАЗДЕЛ__", "__NEWSECTIONLINK__");
		addI18nAlias("nonewsectionlink", "__БЕЗ_ССЫЛКИ_НА_НОВЫЙ_РАЗДЕЛ__", "__NONEWSECTIONLINK__");
		addI18nAlias("currentversion", "ТЕКУЩАЯ_ВЕРСИЯ", "CURRENTVERSION");
		addI18nCIAlias("urlencode", "ЗАКОДИРОВАННЫЙ_АДРЕС:", "URLENCODE:");
		addI18nCIAlias("anchorencode", "КОДИРОВАТЬ_МЕТКУ", "ANCHORENCODE");
		addI18nAlias("currenttimestamp", "ОТМЕТКА_ТЕКУЩЕГО_ВРЕМЕНИ", "CURRENTTIMESTAMP");
		addI18nAlias("localtimestamp", "ОТМЕТКА_МЕСТНОГО_ВРЕМЕНИ", "LOCALTIMESTAMP");
		addI18nAlias("directionmark", "НАПРАВЛЕНИЕ_ПИСЬМА", "DIRECTIONMARK", "DIRMARK");
		addI18nAlias("contentlanguage", "ЯЗЫК_СОДЕРЖАНИЯ", "CONTENTLANGUAGE", "CONTENTLANG");
		addI18nAlias("pagesinnamespace", "СТРАНИЦ_В_ПРОСТРАНСТВЕ_ИМЁН:", "PAGESINNAMESPACE:", "PAGESINNS:");
		addI18nAlias("numberofadmins", "КОЛИЧЕСТВО_АДМИНИСТРАТОРОВ", "NUMBEROFADMINS");
		addI18nCIAlias("formatnum", "ФОРМАТИРОВАТЬ_ЧИСЛО", "FORMATNUM");
		addI18nCIAlias("padleft", "ЗАПОЛНИТЬ_СЛЕВА", "PADLEFT");
		addI18nCIAlias("padright", "ЗАПОЛНИТЬ_СПРАВА", "PADRIGHT");
		addI18nAlias("defaultsort", "СОРТИРОВКА_ПО_УМОЛЧАНИЮ", "КЛЮЧ_СОРТИРОВКИ", "DEFAULTSORT:", "DEFAULTSORTKEY:", "DEFAULTCATEGORYSORT:");
		addI18nCIAlias("filepath", "ПУТЬ_К_ФАЙЛУ:", "FILEPATH:");
		addI18nAlias("hiddencat", "__СКРЫТАЯ_КАТЕГОРИЯ__", "__HIDDENCAT__");
		addI18nAlias("pagesincategory", "СТРАНИЦ_В_КАТЕГОРИИ", "PAGESINCATEGORY", "PAGESINCAT");
		addI18nAlias("pagesize", "РАЗМЕР_СТРАНИЦЫ", "PAGESIZE");
		addI18nAlias("index", "__ИНДЕКС__", "__INDEX__");
		addI18nAlias("numberingroup", "ЧИСЛО_В_ГРУППЕ", "NUMBERINGROUP", "NUMINGROUP");
		addI18nAlias("staticredirect", "__СТАТИЧЕСКОЕ_ПЕРЕНАПРАВЛЕНИЕ__", "__STATICREDIRECT__");
		addI18nAlias("protectionlevel", "УРОВЕНЬ_ЗАЩИТЫ", "PROTECTIONLEVEL");
		addI18nCIAlias("formatdate", "форматдаты", "formatdate", "dateformat");
		addI18nCIAlias("url_path", "ПУТЬ", "PATH");
		addI18nCIAlias("url_wiki", "ВИКИ", "WIKI");
		addI18nCIAlias("url_query", "ЗАПРОС", "QUERY");
		addI18nCIAlias("pagesincategory_all", "все", "all");
		addI18nCIAlias("pagesincategory_pages", "страницы", "pages");
		addI18nCIAlias("pagesincategory_subcats", "подкатегории", "subcats");
		addI18nCIAlias("pagesincategory_files", "файлы", "files");
	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://tt.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "tt";
	}
}
