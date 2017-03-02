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

public class VepConfig extends TemplateConfig {
	public VepConfig() {
		addNamespaceAlias(-2, "Media", "Meedia");
		addNamespaceAlias(-1, "Specialine", "Eri");
		addNamespaceAlias(1, "Lodu", "Arutelu");
		addNamespaceAlias(2, "Kävutai", "Kasutaja");
		addNamespaceAlias(3, "Lodu_kävutajas", "Kasutaja_arutelu");
		addNamespaceAlias(5, "Lodu_Wikipedia-saitas", "{{GRAMMAR:genitive|Wikipedia}}_arutelu", "Wikipedia_arutelu");
		addNamespaceAlias(6, "Fail", "Pilt");
		addNamespaceAlias(7, "Lodu_failas", "Pildi_arutelu");
		addNamespaceAlias(8, "MediaWiki", "MediaWiki");
		addNamespaceAlias(9, "Lodu_MediaWikiš", "MediaWiki_arutelu");
		addNamespaceAlias(10, "Šablon", "Mall");
		addNamespaceAlias(11, "Lodu_šablonas", "Malli_arutelu");
		addNamespaceAlias(12, "Abu", "Juhend");
		addNamespaceAlias(13, "Lodu_abus", "Juhendi_arutelu");
		addNamespaceAlias(14, "Kategorii", "Kategooria");
		addNamespaceAlias(15, "Lodu_kategorijas", "Kategooria_arutelu");

		addI18nAlias("img_right", "oiged", "paremal", "right", "Array");
		addI18nAlias("img_left", "hura", "vasakul", "left", "Array");
		addI18nAlias("img_none", "eile", "tühi", "none", "Array");
		addI18nAlias("img_width", "$1piks", "$1px");
		addI18nAlias("img_border", "röun", "ääris", "border", "Array");
		addI18nAlias("img_top", "üläh", "top");
		addI18nAlias("img_middle", "kesk", "middle");
		addI18nAlias("img_bottom", "ala", "bottom");
		addI18nAlias("sitename", "SAITANNIMI", "KOHANIMI", "SITENAME", "Array");
		addI18nCIAlias("grammar", "GRAMMATIK:", "GRAMMAR:");
		addI18nCIAlias("gender", "SUGU:", "GENDER:", "Array");
		addI18nCIAlias("plural", "ÄILUGU:", "PLURAL:");
		addI18nCIAlias("fullurl", "TÄUZ'URL:", "KOGUURL:", "FULLURL:", "Array");
		addI18nAlias("index", "__INDEKS__", "INDEKSIGA", "__INDEX__", "Array");
		addI18nCIAlias("redirect", "#suuna", "#REDIRECT");
		addI18nCIAlias("notoc", "__SISUKORRATA__", "__NOTOC__");
		addI18nCIAlias("nogallery", "__GALERIITA__", "__NOGALLERY__");
		addI18nCIAlias("forcetoc", "__SISUKORDEES__", "__FORCETOC__");
		addI18nCIAlias("toc", "__SISUKORD__", "__TOC__");
		addI18nCIAlias("noeditsection", "__ALAOSALINGITA__", "__NOEDITSECTION__");
		addI18nAlias("currentmonth", "HETKEKUU", "CURRENTMONTH", "CURRENTMONTH2");
		addI18nAlias("currentmonth1", "HETKEKUU1", "CURRENTMONTH1");
		addI18nAlias("currentmonthname", "HETKEKUUNIMETUS", "CURRENTMONTHNAME");
		addI18nAlias("currentday", "HETKEKUUPÄEV", "CURRENTDAY");
		addI18nAlias("currentday2", "HETKEKUUPÄEV2", "CURRENTDAY2");
		addI18nAlias("currentdayname", "HETKENÄDALAPÄEV", "CURRENTDAYNAME");
		addI18nAlias("currentyear", "HETKEAASTA", "CURRENTYEAR");
		addI18nAlias("currenttime", "HETKEAEG", "CURRENTTIME");
		addI18nAlias("currenthour", "HETKETUND", "CURRENTHOUR");
		addI18nAlias("localmonth", "KOHALIKKUU", "LOCALMONTH", "LOCALMONTH2");
		addI18nAlias("localmonth1", "KOHALIKKUU1", "LOCALMONTH1");
		addI18nAlias("localmonthname", "KOHALIKKUUNIMETUS", "LOCALMONTHNAME");
		addI18nAlias("localday", "KOHALIKKUUPÄEV", "LOCALDAY");
		addI18nAlias("localday2", "KOHALIKKUUPÄEV2", "LOCALDAY2");
		addI18nAlias("localdayname", "KOHALIKNÄDALAPÄEV", "LOCALDAYNAME");
		addI18nAlias("localyear", "KOHALIKAASTA", "LOCALYEAR");
		addI18nAlias("localtime", "KOHALIKAEG", "LOCALTIME");
		addI18nAlias("localhour", "KOHALIKTUND", "LOCALHOUR");
		addI18nAlias("numberofpages", "LEHEMÄÄR", "NUMBEROFPAGES");
		addI18nAlias("numberofarticles", "ARTIKLIMÄÄR", "NUMBEROFARTICLES");
		addI18nAlias("numberoffiles", "FAILIMÄÄR", "NUMBEROFFILES");
		addI18nAlias("numberofusers", "KASUTAJAMÄÄR", "NUMBEROFUSERS");
		addI18nAlias("numberofactiveusers", "TEGUSKASUTAJAMÄÄR", "NUMBEROFACTIVEUSERS");
		addI18nAlias("numberofedits", "REDIGEERIMISMÄÄR", "NUMBEROFEDITS");
		addI18nAlias("numberofviews", "VAATAMISTEARV", "NUMBEROFVIEWS");
		addI18nAlias("pagename", "LEHEKÜLJENIMI", "PAGENAME");
		addI18nAlias("pagenamee", "LEHEKÜLJENIMI1", "PAGENAMEE");
		addI18nAlias("namespace", "NIMERUUM", "NAMESPACE");
		addI18nAlias("namespacee", "NIMERUUM1", "NAMESPACEE");
		addI18nAlias("namespacenumber", "NIMERUUMINUMBER", "NAMESPACENUMBER");
		addI18nAlias("talkspace", "ARUTELUNIMERUUM", "TALKSPACE");
		addI18nAlias("talkspacee", "ARUTELUNIMERUUM1", "TALKSPACEE");
		addI18nAlias("subjectspace", "SISUNIMERUUM", "SUBJECTSPACE", "ARTICLESPACE");
		addI18nAlias("subjectspacee", "SISUNIMERUUM1", "SUBJECTSPACEE", "ARTICLESPACEE");
		addI18nAlias("fullpagename", "KOGULEHEKÜLJENIMI", "FULLPAGENAME");
		addI18nAlias("fullpagenamee", "KOGULEHEKÜLJENIMI1", "FULLPAGENAMEE");
		addI18nAlias("subpagename", "ALAMLEHEKÜLJENIMI", "SUBPAGENAME");
		addI18nAlias("subpagenamee", "ALAMLEHEKÜLJENIMI1", "SUBPAGENAMEE");
		addI18nAlias("rootpagename", "JUURLEHEKÜLJENIMI", "ROOTPAGENAME");
		addI18nAlias("rootpagenamee", "JUURLEHEKÜLJENIMI1", "ROOTPAGENAMEE");
		addI18nAlias("basepagename", "NIMERUUMITANIMI", "BASEPAGENAME");
		addI18nAlias("basepagenamee", "NIMERUUMITANIMI1", "BASEPAGENAMEE");
		addI18nAlias("talkpagename", "ARUTELUNIMI", "TALKPAGENAME");
		addI18nAlias("talkpagenamee", "ARUTELUNIMI1", "TALKPAGENAMEE");
		addI18nCIAlias("subst", "ASENDA:", "SUBST:");
		addI18nAlias("img_thumbnail", "pisi", "pisipilt", "thumbnail", "thumb");
		addI18nAlias("img_manualthumb", "pisi=$1", "pisipilt=$1", "thumbnail=$1", "thumb=$1");
		addI18nAlias("img_center", "keskel", "center", "centre");
		addI18nAlias("img_framed", "raam", "framed", "enframed", "frame");
		addI18nAlias("img_frameless", "raamita", "frameless");
		addI18nAlias("img_page", "lehekülg=$1", "lehekülg_$1", "page=$1", "page $1");
		addI18nCIAlias("ns", "NR:", "NS:");
		addI18nCIAlias("nse", "NR1:", "NSE:");
		addI18nCIAlias("localurl", "KOHALIKURL", "LOCALURL:");
		addI18nCIAlias("localurle", "KOHALIKURL1", "LOCALURLE:");
		addI18nCIAlias("servername", "SERVERINIMI", "SERVERNAME");
		addI18nAlias("currentweek", "HETKENÄDAL", "CURRENTWEEK");
		addI18nAlias("currentdow", "HETKENÄDALAPÄEV1", "CURRENTDOW");
		addI18nAlias("localweek", "KOHALIKNÄDAL", "LOCALWEEK");
		addI18nAlias("localdow", "KOHALIKNÄDALAPÄEV1", "LOCALDOW");
		addI18nCIAlias("fullurle", "KOGUURL1:", "FULLURLE:");
		addI18nCIAlias("lcfirst", "ESIVT:", "LCFIRST:");
		addI18nCIAlias("ucfirst", "ESIST:", "UCFIRST:");
		addI18nCIAlias("lc", "VT:", "LC:");
		addI18nCIAlias("uc", "ST:", "UC:");
		addI18nAlias("displaytitle", "PEALKIRI", "DISPLAYTITLE");
		addI18nAlias("newsectionlink", "__UUEALAOSALINK__", "__NEWSECTIONLINK__");
		addI18nAlias("nonewsectionlink", "__UUEALAOSALINGITA__", "__NONEWSECTIONLINK__");
		addI18nAlias("currenttimestamp", "HETKEAJATEMPEL", "CURRENTTIMESTAMP");
		addI18nAlias("localtimestamp", "KOHALIKAJATEMPEL", "LOCALTIMESTAMP");
		addI18nCIAlias("language", "#KEEL:", "#LANGUAGE:");
		addI18nAlias("contentlanguage", "VAIKEKEEL", "CONTENTLANGUAGE", "CONTENTLANG");
		addI18nAlias("pagesinnamespace", "LEHEKÜLGINIMERUUMIS", "PAGESINNAMESPACE:", "PAGESINNS:");
		addI18nAlias("numberofadmins", "ÜLEMAMÄÄR", "NUMBEROFADMINS");
		addI18nCIAlias("formatnum", "ARVUVORMINDUS", "FORMATNUM");
		addI18nCIAlias("special", "eri", "special");
		addI18nCIAlias("speciale", "eri1", "speciale");
		addI18nAlias("defaultsort", "JÄRJESTA:", "DEFAULTSORT:", "DEFAULTSORTKEY:", "DEFAULTCATEGORYSORT:");
		addI18nCIAlias("filepath", "FAILITEE:", "FILEPATH:");
		addI18nAlias("hiddencat", "__PEIDETUDKAT__", "__HIDDENCAT__");
		addI18nAlias("pagesincategory", "LEHEKÜLGIKATEGOORIAS", "PAGESINCATEGORY", "PAGESINCAT");
		addI18nAlias("noindex", "INDEKSITA", "__NOINDEX__");
		addI18nAlias("numberingroup", "KASUTAJAIDRÜHMAS", "NUMBERINGROUP", "NUMINGROUP");
		addI18nAlias("protectionlevel", "KAITSETASE", "PROTECTIONLEVEL");
		addI18nCIAlias("formatdate", "kuupäevavormindus", "formatdate", "dateformat");
	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://vep.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "vep";
	}
}
