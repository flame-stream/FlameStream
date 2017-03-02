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

public class MznConfig extends TemplateConfig {
	public MznConfig() {
		addNamespaceAlias(-2, "مدیا", "مه‌دیا", "مدیا", "مدیا", "رسانه", "رسانه‌ای");
		addNamespaceAlias(-1, "شا", "ویژه", "ویژه");
		addNamespaceAlias(0, "", "");
		addNamespaceAlias(1, "گپ", "بحث", "بحث");
		addNamespaceAlias(2, "کارور", "کاربر", "کاربر");
		addNamespaceAlias(3, "کارور_گپ", "بحث_کاربر", "بحث_کاربر");
		addNamespaceAlias(5, "Wikipedia_گپ", "بحث_Wikipedia", "بحث_Wikipedia");
		addNamespaceAlias(6, "پرونده", "تصویر", "پرونده", "پرونده", "تصویر");
		addNamespaceAlias(7, "پرونده_گپ", "بحث_تصویر", "بحث_پرونده", "بحث_پرونده", "بحث_تصویر");
		addNamespaceAlias(8, "مدیاویکی", "مدیاویکی", "مه‌دیا ویکی", "مه‌دیاویکی", "مدیاویکی");
		addNamespaceAlias(9, "مدیاویکی_گپ", "مه‌دیاویکی_گپ", "بحث_مدیاویکی", "مه‌دیا ویکی گپ", "بحث_مدیاویکی");
		addNamespaceAlias(10, "شابلون", "الگو", "الگو");
		addNamespaceAlias(11, "شابلون_گپ", "بحث_الگو", "بحث_الگو");
		addNamespaceAlias(12, "رانما", "راهنما", "رانه‌ما", "راهنما");
		addNamespaceAlias(13, "رانما_گپ", "رانه‌مائه_گپ", "بحث_راهنما", "رانه‌مای گپ", "بحث_راهنما");
		addNamespaceAlias(14, "رج", "رده", "رده");
		addNamespaceAlias(15, "رج_گپ", "بحث_رده", "بحث_رده");

		addI18nCIAlias("redirect", "#بور", "#تغییرمسیر", "#تغییر_مسیر", "#REDIRECT", "Array");
		addI18nCIAlias("notoc", "__بی‌فهرست__", "__NOTOC__", "Array");
		addI18nCIAlias("nogallery", "__بی‌نگارخنه__", "__بی‌نگارخانه__", "__NOGALLERY__", "Array");
		addI18nCIAlias("forcetoc", "__بافهرست__", "__FORCETOC__", "Array");
		addI18nCIAlias("toc", "__فهرست__", "__TOC__", "Array");
		addI18nCIAlias("noeditsection", "__بی‌بخش__", "__NOEDITSECTION__", "Array");
		addI18nAlias("currentmonth", "ماه", "ماه‌کنونی", "ماه_کنونی", "ماه‌کنونی۲", "ماه_اسایی۲", "ماه_کنونی۲", "CURRENTMONTH", "CURRENTMONTH2", "Array");
		addI18nAlias("currentmonth1", "ماه۱", "ماه‌کنونی۱", "ماه_کنونی۱", "CURRENTMONTH1", "Array");
		addI18nAlias("currentmonthname", "نام‌ماه", "نام_ماه", "نام‌ماه‌کنونی", "نام_ماه_کنونی", "CURRENTMONTHNAME", "Array");
		addI18nAlias("currentmonthnamegen", "نام‌ماه‌اضافه", "نام_ماه_اضافه", "نام‌ماه‌کنونی‌اضافه", "نام_ماه_کنونی_اضافه", "CURRENTMONTHNAMEGEN", "Array");
		addI18nAlias("currentmonthabbrev", "مخفف‌نام‌ماه", "مخفف_نام_ماه", "CURRENTMONTHABBREV", "Array");
		addI18nAlias("currentday", "روز", "روزکنونی", "روز_کنونی", "CURRENTDAY", "Array");
		addI18nAlias("currentday2", "روز۲", "روز_۲", "CURRENTDAY2", "Array");
		addI18nAlias("currentdayname", "نام‌روز", "نام_روز", "CURRENTDAYNAME", "Array");
		addI18nAlias("currentyear", "سال", "سال‌کنونی", "سال_کنونی", "CURRENTYEAR", "Array");
		addI18nAlias("currenttime", "زمان‌کنونی", "زمان_کنونی", "CURRENTTIME", "Array");
		addI18nAlias("currenthour", "ساعت", "ساعت‌کنونی", "ساعت_کنونی", "CURRENTHOUR", "Array");
		addI18nAlias("localmonth", "ماه‌محلی", "ماه_محلی", "ماه‌محلی۲", "ماه_محلی۲", "LOCALMONTH", "LOCALMONTH2", "Array");
		addI18nAlias("localmonth1", "ماه‌محلی۱", "ماه_محلی۱", "LOCALMONTH1", "Array");
		addI18nAlias("localmonthname", "نام‌ماه‌محلی", "نام_ماه_محلی", "LOCALMONTHNAME", "Array");
		addI18nAlias("localmonthnamegen", "نام‌ماه‌محلی‌اضافه", "نام_ماه_محلی_اضافه", "LOCALMONTHNAMEGEN", "Array");
		addI18nAlias("localmonthabbrev", "مخفف‌ماه‌محلی", "مخفف_ماه_محلی", "LOCALMONTHABBREV", "Array");
		addI18nAlias("localday", "روزمحلی", "روز_محلی", "LOCALDAY", "Array");
		addI18nAlias("localday2", "روزمحلی۲", "روز_محلی_۲", "LOCALDAY2", "Array");
		addI18nAlias("localdayname", "نام‌روزمحلی", "نام_روز_محلی", "LOCALDAYNAME", "Array");
		addI18nAlias("localyear", "سال‌محلی", "سال_محلی", "LOCALYEAR", "Array");
		addI18nAlias("localtime", "زمون_محلی", "زمان_محلی", "زمان‌محلی", "LOCALTIME", "Array");
		addI18nAlias("localhour", "ساعت‌محلی", "ساعت_محلی", "LOCALHOUR", "Array");
		addI18nAlias("numberofpages", "تعدادصفحه‌ها", "تعداد_صفحه‌ها", "ولگ‌ئون_نمره", "وألگ‌ئون_نومره", "NUMBEROFPAGES", "Array");
		addI18nAlias("numberofarticles", "تعدادمقاله‌ها", "NUMBEROFARTICLES", "Array");
		addI18nAlias("numberoffiles", "تعدادپرونده‌ها", "NUMBEROFFILES", "Array");
		addI18nAlias("numberofusers", "تعدادکارورون", "تعدادکاربران", "NUMBEROFUSERS", "Array");
		addI18nAlias("numberofactiveusers", "کارورون_فعال", "کاربران_فعال", "کاربران‌فعال", "NUMBEROFACTIVEUSERS", "Array");
		addI18nAlias("numberofedits", "تعداددچی‌یه‌ئون", "تعدادویرایش‌ها", "NUMBEROFEDITS", "Array");
		addI18nAlias("numberofviews", "تعدادبازدید", "NUMBEROFVIEWS", "Array");
		addI18nAlias("pagename", "نام‌صفحه", "نام_صفحه", "PAGENAME", "Array");
		addI18nAlias("pagenamee", "نام‌صفحه‌کد", "نام_صفحه_کد", "PAGENAMEE", "Array");
		addI18nAlias("namespace", "فضای‌نام", "فضای_نام", "NAMESPACE", "Array");
		addI18nAlias("namespacee", "فضای‌نام‌کد", "فضای_نام_کد", "NAMESPACEE", "Array");
		addI18nAlias("talkspace", "فضای‌گپ", "فضای_گپ", "فضای‌بحث", "فضای_بحث", "TALKSPACE", "Array");
		addI18nAlias("talkspacee", "فضای‌گپ_کد", "فضای_گپ_کد", "فضای‌بحث‌کد", "فضای_بحث_کد", "TALKSPACEE", "Array");
		addI18nAlias("subjectspace", "فضای‌موضوع", "فضای‌مقاله", "فضای_موضوع", "فضای_مقاله", "SUBJECTSPACE", "ARTICLESPACE", "Array");
		addI18nCIAlias("int", "ترجمه:", "INT:", "Array");
		addI18nAlias("sitename", "نام‌وبگاه", "نام_وبگاه", "SITENAME", "Array");
		addI18nCIAlias("ns", "فن:", "NS:", "Array");
		addI18nCIAlias("nse", "فنک:", "NSE:", "Array");
		addI18nCIAlias("localurl", "نشونی:", "نشانی:", "LOCALURL:", "Array");
		addI18nCIAlias("grammar", "دستور_زبون:", "دستور_زوون:", "دستورزبان:", "دستور_زبان:", "GRAMMAR:", "Array");
		addI18nCIAlias("gender", "جنسیت:", "جنس:", "GENDER:", "Array");
		addI18nAlias("namespacenumber", "شماره_فضای_نام", "شماره‌فضای‌نام", "NAMESPACENUMBER");
		addI18nAlias("subjectspacee", "فضای‌موضوع‌کد", "فضای‌مقاله‌کد", "فضای_موضوع_کد", "فضای_مقاله_کد", "SUBJECTSPACEE", "ARTICLESPACEE");
		addI18nAlias("fullpagename", "نام‌کامل‌صفحه", "نام_کامل_صفحه", "FULLPAGENAME");
		addI18nAlias("fullpagenamee", "نام‌کامل‌صفحه‌کد", "نام_کامل_صفحه_کد", "FULLPAGENAMEE");
		addI18nAlias("subpagename", "نام‌زیرصفحه", "نام_زیرصفحه", "SUBPAGENAME");
		addI18nAlias("subpagenamee", "نام‌زیرصفحه‌کد", "نام_زیرصفحه_کد", "SUBPAGENAMEE");
		addI18nAlias("rootpagename", "نام_صفحه_ریشه", "ROOTPAGENAME");
		addI18nAlias("rootpagenamee", "نام_صفحه_ریشه_ای", "ROOTPAGENAMEE");
		addI18nAlias("basepagename", "نام‌صفحه‌مبنا", "نام_صفحه_مبنا", "BASEPAGENAME");
		addI18nAlias("basepagenamee", "نام‌صفحه‌مبناکد", "نام_صفحه_مبنا_کد", "BASEPAGENAMEE");
		addI18nAlias("talkpagename", "نام‌صفحه‌بحث", "نام_صفحه_بحث", "TALKPAGENAME");
		addI18nAlias("talkpagenamee", "نام‌صفحه‌بحث‌کد", "نام_صفحه_بحث_کد", "TALKPAGENAMEE");
		addI18nAlias("subjectpagename", "نام‌صفحه‌موضوع", "نام‌صفحه‌مقاله", "نام_صفحه_موضوع", "نام_صفحه_مقاله", "SUBJECTPAGENAME", "ARTICLEPAGENAME");
		addI18nAlias("subjectpagenamee", "نام‌صفحه‌موضوع‌کد", "نام‌صفحه‌مقاله‌کد", "نام_صفحه_موضوع_کد", "نام_صفحه_مقاله_کد", "SUBJECTPAGENAMEE", "ARTICLEPAGENAMEE");
		addI18nCIAlias("msg", "پیغام:", "پ:", "MSG:");
		addI18nCIAlias("subst", "جایگزین:", "جا:", "SUBST:");
		addI18nCIAlias("safesubst", "جایگزین_امن:", "جام:", "SAFESUBST:");
		addI18nCIAlias("msgnw", "پیغام‌بی‌بسط:", "MSGNW:");
		addI18nAlias("img_thumbnail", "بندانگشتی", "انگشتدان", "انگشتی", "thumbnail", "thumb");
		addI18nAlias("img_manualthumb", "بندانگشتی=$1", "انگشتدان=$1", "انگشتی=$1", "thumbnail=$1", "thumb=$1");
		addI18nAlias("img_right", "راست", "right");
		addI18nAlias("img_left", "چپ", "left");
		addI18nAlias("img_none", "هیچ", "none");
		addI18nAlias("img_width", "$1پیکسل", "$1px");
		addI18nAlias("img_center", "وسط", "center", "centre");
		addI18nAlias("img_framed", "قاب", "framed", "enframed", "frame");
		addI18nAlias("img_frameless", "بی‌قاب", "بیقاب", "بی_قاب", "frameless");
		addI18nAlias("img_lang", "زبان=$1", "lang=$1");
		addI18nAlias("img_page", "صفحه=$1", "صفحه_$1", "page=$1", "page $1");
		addI18nAlias("img_upright", "ایستاده", "ایستاده=$1", "ایستاده_$1", "upright", "upright=$1", "upright $1");
		addI18nAlias("img_border", "حاشیه", "border");
		addI18nAlias("img_baseline", "همکف", "baseline");
		addI18nAlias("img_sub", "زیر", "sub");
		addI18nAlias("img_super", "زبر", "super", "sup");
		addI18nAlias("img_top", "بالا", "top");
		addI18nAlias("img_text_top", "متن-بالا", "text-top");
		addI18nAlias("img_middle", "میانه", "middle");
		addI18nAlias("img_bottom", "پایین", "bottom");
		addI18nAlias("img_text_bottom", "متن-پایین", "text-bottom");
		addI18nAlias("img_link", "پیوند=$1", "link=$1");
		addI18nAlias("img_alt", "جایگزین=$1", "alt=$1");
		addI18nAlias("img_class", "کلاس=$1", "class=$1");
		addI18nCIAlias("localurle", "نشانی‌کد:", "نشانی_کد:", "LOCALURLE:");
		addI18nCIAlias("articlepath", "مسیرمقاله", "مسیر_مقاله", "ARTICLEPATH");
		addI18nCIAlias("pageid", "شناسه_صفحه", "PAGEID");
		addI18nCIAlias("server", "سرور", "کارساز", "SERVER");
		addI18nCIAlias("servername", "نام‌کارساز", "نام_کارساز", "نام‌سرور", "نام_سرور", "SERVERNAME");
		addI18nCIAlias("scriptpath", "مسیرسند", "مسیر_سند", "SCRIPTPATH");
		addI18nCIAlias("stylepath", "مسیرسبک", "مسیر_سبک", "STYLEPATH");
		addI18nCIAlias("notitleconvert", "__عنوان‌تبدیل‌نشده__", "__NOTITLECONVERT__", "__NOTC__");
		addI18nCIAlias("nocontentconvert", "__محتواتبدیل‌نشده__", "__NOCONTENTCONVERT__", "__NOCC__");
		addI18nAlias("currentweek", "هفته", "CURRENTWEEK");
		addI18nAlias("currentdow", "روزهفته", "روز_هفته", "CURRENTDOW");
		addI18nAlias("localweek", "هفته‌محلی", "هفته_محلی", "LOCALWEEK");
		addI18nAlias("localdow", "روزهفته‌محلی", "روز_هفته_محلی", "LOCALDOW");
		addI18nAlias("revisionid", "نسخه", "شماره‌نسخه", "شماره_نسخه", "REVISIONID");
		addI18nAlias("revisionday", "روزنسخه", "روز_نسخه", "REVISIONDAY");
		addI18nAlias("revisionday2", "روزنسخه۲", "روز_نسخه۲", "روز_نسخه_۲", "REVISIONDAY2");
		addI18nAlias("revisionmonth", "ماه‌نسخه", "ماه_نسخه", "REVISIONMONTH");
		addI18nAlias("revisionmonth1", "ماه‌نسخه۱", "ماه_نسخه_۱", "REVISIONMONTH1");
		addI18nAlias("revisionyear", "سال‌نسخه", "سال_نسخه", "REVISIONYEAR");
		addI18nAlias("revisiontimestamp", "زمان‌یونیکسی‌نسخه", "زمان‌نسخه", "زمان_یونیکسی_نسخه", "زمان_نسخه", "REVISIONTIMESTAMP");
		addI18nAlias("revisionuser", "کاربرنسخه", "کاربر_نسخه", "REVISIONUSER");
		addI18nCIAlias("plural", "جمع:", "PLURAL:");
		addI18nCIAlias("fullurl", "نشانی‌کامل:", "نشانی_کامل:", "FULLURL:");
		addI18nCIAlias("fullurle", "نشانی‌کامل‌کد:", "نشانی_کامل_کد:", "FULLURLE:");
		addI18nCIAlias("canonicalurl", "نشانی_استاندارد:", "نشانی‌استاندارد:", "CANONICALURL:");
		addI18nCIAlias("lcfirst", "ابتداکوچک:", "ابتدا_کوچک:", "LCFIRST:");
		addI18nCIAlias("ucfirst", "ابتدابزرگ:", "ابتدا_بزرگ:", "UCFIRST:");
		addI18nCIAlias("lc", "ک:", "LC:");
		addI18nCIAlias("uc", "ب:", "UC:");
		addI18nCIAlias("raw", "خام:", "RAW:");
		addI18nAlias("displaytitle", "عنوان‌ظاهری", "عنوان_ظاهری", "DISPLAYTITLE");
		addI18nAlias("rawsuffix", "ن", "R");
		addI18nAlias("newsectionlink", "__بخش‌جدید__", "__NEWSECTIONLINK__");
		addI18nAlias("nonewsectionlink", "__بی‌پیوندبخش__", "__بی‌پیوند‌بخش‌جدید__", "__NONEWSECTIONLINK__");
		addI18nAlias("currentversion", "نسخه‌کنونی", "نسخه_کنونی", "CURRENTVERSION");
		addI18nCIAlias("urlencode", "کدنشانی:", "URLENCODE:");
		addI18nCIAlias("anchorencode", "کدلنگر:", "ANCHORENCODE");
		addI18nAlias("currenttimestamp", "زمان‌یونیکسی", "زمان_یونیکسی", "CURRENTTIMESTAMP");
		addI18nAlias("localtimestamp", "زمان‌یونیکسی‌محلی", "زمان_یونیکسی_محلی", "LOCALTIMESTAMP");
		addI18nAlias("directionmark", "علامت‌جهت", "علامت_جهت", "DIRECTIONMARK", "DIRMARK");
		addI18nCIAlias("language", "#زبان:", "#LANGUAGE:");
		addI18nAlias("contentlanguage", "زبان‌محتوا", "زبان_محتوا", "CONTENTLANGUAGE", "CONTENTLANG");
		addI18nAlias("pagesinnamespace", "صفحه‌درفضای‌نام:", "صفحه_در_فضای_نام:", "PAGESINNAMESPACE:", "PAGESINNS:");
		addI18nAlias("numberofadmins", "تعدادمدیران", "NUMBEROFADMINS");
		addI18nCIAlias("formatnum", "آرایش‌عدد", "آرایش_عدد", "FORMATNUM");
		addI18nCIAlias("padleft", "لبه‌چپ", "لبه_چپ", "PADLEFT");
		addI18nCIAlias("padright", "لبه‌راست", "لبه_راست", "PADRIGHT");
		addI18nCIAlias("special", "ویژه", "special");
		addI18nCIAlias("speciale", "ویژه_ای", "speciale");
		addI18nAlias("defaultsort", "ترتیب:", "ترتیب‌پیش‌فرض:", "ترتیب_پیش_فرض:", "DEFAULTSORT:", "DEFAULTSORTKEY:", "DEFAULTCATEGORYSORT:");
		addI18nCIAlias("filepath", "مسیرپرونده:", "مسیر_پرونده:", "FILEPATH:");
		addI18nCIAlias("tag", "برچسب", "tag");
		addI18nAlias("hiddencat", "__رده‌پنهان__", "__HIDDENCAT__");
		addI18nAlias("pagesincategory", "صفحه‌دررده", "صفحه_در_رده", "PAGESINCATEGORY", "PAGESINCAT");
		addI18nAlias("pagesize", "اندازه‌صفحه", "اندازه_صفحه", "PAGESIZE");
		addI18nAlias("index", "__نمایه__", "__INDEX__");
		addI18nAlias("noindex", "__بی‌نمایه__", "__NOINDEX__");
		addI18nAlias("numberingroup", "تعداددرگروه", "NUMBERINGROUP", "NUMINGROUP");
		addI18nAlias("staticredirect", "__تغییرمسیرثابت__", "__STATICREDIRECT__");
		addI18nAlias("protectionlevel", "سطح‌حفاطت", "سطح_حفاظت", "PROTECTIONLEVEL");
		addI18nCIAlias("formatdate", "آرایش‌تاریخ", "آرایش_تاریخ", "formatdate", "dateformat");
		addI18nCIAlias("url_path", "مسیر", "PATH");
		addI18nCIAlias("url_wiki", "ویکی", "WIKI");
		addI18nCIAlias("url_query", "دستور", "QUERY");
		addI18nCIAlias("defaultsort_noerror", "بدون‌خطا", "بدون_خطا", "noerror");
		addI18nCIAlias("defaultsort_noreplace", "جایگزین‌نکن", "جایگزین_نکن", "noreplace");
		addI18nCIAlias("pagesincategory_all", "همه", "all");
		addI18nCIAlias("pagesincategory_pages", "صفحات", "pages");
		addI18nCIAlias("pagesincategory_subcats", "زیررده‌ها", "subcats");
		addI18nCIAlias("pagesincategory_files", "پرونده‌ها", "files");
	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://mzn.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "mzn";
	}
}
