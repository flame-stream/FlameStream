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

public class SahConfig extends TemplateConfig {
	public SahConfig() {
		addNamespaceAlias(-1, "Аналлаах", "Служебная");
		addNamespaceAlias(1, "Ырытыы", "Обсуждение");
		addNamespaceAlias(2, "Кыттааччы", "Участник", "Участник", "Участница");
		addNamespaceAlias(3, "Кыттааччы_ырытыыта", "Обсуждение_участника", "Обсуждение_участника", "Обсуждение_участницы");
		addNamespaceAlias(5, "Wikipedia_ырытыыта", "Обсуждение_{{GRAMMAR:genitive|Wikipedia}}");
		addNamespaceAlias(6, "Билэ", "Ойуу", "Файл", "Изображение");
		addNamespaceAlias(7, "Билэ_ырытыыта", "Ойуу_ырытыыта", "Обсуждение_файла", "Обсуждение_изображения");
		addNamespaceAlias(10, "Халыып", "Шаблон");
		addNamespaceAlias(11, "Халыып_ырытыыта", "Обсуждение_шаблона");
		addNamespaceAlias(12, "Көмө", "Справка");
		addNamespaceAlias(13, "Көмө_ырытыыта", "Обсуждение_справки");
		addNamespaceAlias(14, "Категория", "Категория");
		addNamespaceAlias(15, "Категория_ырытыыта", "Обсуждение_категории");
		addNamespaceAlias(-2, "Медиа");
		addNamespaceAlias(8, "MediaWiki");
		addNamespaceAlias(9, "Обсуждение_MediaWiki");

		addI18nCIAlias("redirect", "#перенаправление", "#перенапр", "#REDIRECT");
		addI18nCIAlias("notoc", "__БЕЗ_ОГЛАВЛЕНИЯ__", "__БЕЗ_ОГЛ__", "__NOTOC__");
		addI18nCIAlias("nogallery", "__БЕЗ_ГАЛЕРЕИ__", "__NOGALLERY__");
		addI18nCIAlias("forcetoc", "__ОБЯЗАТЕЛЬНОЕ_ОГЛАВЛЕНИЕ__", "__ОБЯЗ_ОГЛ__", "__FORCETOC__");
		addI18nCIAlias("toc", "__ОГЛАВЛЕНИЕ__", "__ОГЛ__", "__TOC__");
		addI18nCIAlias("noeditsection", "__БЕЗ_РЕДАКТИРОВАНИЯ_РАЗДЕЛА__", "__NOEDITSECTION__");
		addI18nAlias("currentmonth", "ТЕКУЩИЙ_МЕСЯЦ", "ТЕКУЩИЙ_МЕСЯЦ_2", "CURRENTMONTH", "CURRENTMONTH2");
		addI18nAlias("currentmonth1", "ТЕКУЩИЙ_МЕСЯЦ_1", "CURRENTMONTH1");
		addI18nAlias("currentmonthname", "НАЗВАНИЕ_ТЕКУЩЕГО_МЕСЯЦА", "CURRENTMONTHNAME");
		addI18nAlias("currentmonthnamegen", "НАЗВАНИЕ_ТЕКУЩЕГО_МЕСЯЦА_РОД", "CURRENTMONTHNAMEGEN");
		addI18nAlias("currentmonthabbrev", "НАЗВАНИЕ_ТЕКУЩЕГО_МЕСЯЦА_АБР", "CURRENTMONTHABBREV");
		addI18nAlias("currentday", "ТЕКУЩИЙ_ДЕНЬ", "CURRENTDAY");
		addI18nAlias("currentday2", "ТЕКУЩИЙ_ДЕНЬ_2", "CURRENTDAY2");
		addI18nAlias("currentdayname", "НАЗВАНИЕ_ТЕКУЩЕГО_ДНЯ", "CURRENTDAYNAME");
		addI18nAlias("currentyear", "ТЕКУЩИЙ_ГОД", "CURRENTYEAR");
		addI18nAlias("currenttime", "ТЕКУЩЕЕ_ВРЕМЯ", "CURRENTTIME");
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
		addI18nAlias("numberofarticles", "КОЛИЧЕСТВО_СТАТЕЙ", "NUMBEROFARTICLES");
		addI18nAlias("numberoffiles", "КОЛИЧЕСТВО_ФАЙЛОВ", "NUMBEROFFILES");
		addI18nAlias("numberofusers", "КОЛИЧЕСТВО_УЧАСТНИКОВ", "NUMBEROFUSERS");
		addI18nAlias("numberofactiveusers", "КОЛИЧЕСТВО_АКТИВНЫХ_УЧАСТНИКОВ", "NUMBEROFACTIVEUSERS");
		addI18nAlias("numberofedits", "КОЛИЧЕСТВО_ПРАВОК", "NUMBEROFEDITS");
		addI18nAlias("numberofviews", "КОЛИЧЕСТВО_ПРОСМОТРОВ", "NUMBEROFVIEWS");
		addI18nAlias("pagename", "НАЗВАНИЕ_СТРАНИЦЫ", "PAGENAME");
		addI18nAlias("pagenamee", "НАЗВАНИЕ_СТРАНИЦЫ_2", "PAGENAMEE");
		addI18nAlias("namespace", "ПРОСТРАНСТВО_ИМЁН", "NAMESPACE");
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
		addI18nCIAlias("msg", "СООБЩЕНИЕ:", "СООБЩ:", "MSG:");
		addI18nCIAlias("subst", "ПОДСТАНОВКА:", "ПОДСТ:", "SUBST:");
		addI18nCIAlias("safesubst", "ЗАЩПОДСТ:", "SAFESUBST:");
		addI18nCIAlias("msgnw", "СООБЩ_БЕЗ_ВИКИ:", "MSGNW:");
		addI18nAlias("img_thumbnail", "мини", "миниатюра", "thumbnail", "thumb");
		addI18nAlias("img_manualthumb", "мини=$1", "миниатюра=$1", "thumbnail=$1", "thumb=$1");
		addI18nAlias("img_right", "справа", "right");
		addI18nAlias("img_left", "слева", "left");
		addI18nAlias("img_none", "без", "none");
		addI18nAlias("img_width", "$1пкс", "$1px");
		addI18nAlias("img_center", "центр", "center", "centre");
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
		addI18nCIAlias("int", "ВНУТР:", "INT:");
		addI18nAlias("sitename", "НАЗВАНИЕ_САЙТА", "SITENAME");
		addI18nCIAlias("ns", "ПИ:", "NS:");
		addI18nCIAlias("nse", "ПИК:", "NSE:");
		addI18nCIAlias("localurl", "ЛОКАЛЬНЫЙ_АДРЕС:", "LOCALURL:");
		addI18nCIAlias("localurle", "ЛОКАЛЬНЫЙ_АДРЕС_2:", "LOCALURLE:");
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
		addI18nCIAlias("language", "#ЯЗЫК:", "#LANGUAGE:");
		addI18nAlias("contentlanguage", "ЯЗЫК_СОДЕРЖАНИЯ", "CONTENTLANGUAGE", "CONTENTLANG");
		addI18nAlias("pagesinnamespace", "СТРАНИЦ_В_ПРОСТРАНСТВЕ_ИМЁН:", "PAGESINNAMESPACE:", "PAGESINNS:");
		addI18nAlias("numberofadmins", "КОЛИЧЕСТВО_АДМИНИСТРАТОРОВ", "NUMBEROFADMINS");
		addI18nCIAlias("formatnum", "ФОРМАТИРОВАТЬ_ЧИСЛО", "FORMATNUM");
		addI18nCIAlias("padleft", "ЗАПОЛНИТЬ_СЛЕВА", "PADLEFT");
		addI18nCIAlias("padright", "ЗАПОЛНИТЬ_СПРАВА", "PADRIGHT");
		addI18nCIAlias("special", "служебная", "special");
		addI18nAlias("defaultsort", "СОРТИРОВКА_ПО_УМОЛЧАНИЮ", "КЛЮЧ_СОРТИРОВКИ", "DEFAULTSORT:", "DEFAULTSORTKEY:", "DEFAULTCATEGORYSORT:");
		addI18nCIAlias("filepath", "ПУТЬ_К_ФАЙЛУ:", "FILEPATH:");
		addI18nCIAlias("tag", "метка", "тег", "тэг", "tag");
		addI18nAlias("hiddencat", "__СКРЫТАЯ_КАТЕГОРИЯ__", "__HIDDENCAT__");
		addI18nAlias("pagesincategory", "СТРАНИЦ_В_КАТЕГОРИИ", "PAGESINCATEGORY", "PAGESINCAT");
		addI18nAlias("pagesize", "РАЗМЕР_СТРАНИЦЫ", "PAGESIZE");
		addI18nAlias("index", "__ИНДЕКС__", "__INDEX__");
		addI18nAlias("noindex", "__БЕЗ_ИНДЕКСА__", "__NOINDEX__");
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
		return "http://sah.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "sah";
	}
}
