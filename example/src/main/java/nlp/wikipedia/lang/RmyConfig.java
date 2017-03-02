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

public class RmyConfig extends TemplateConfig {
	public RmyConfig() {
		addNamespaceAlias(-2, "Mediya", "Media");
		addNamespaceAlias(-1, "Uzalutno", "Special");
		addNamespaceAlias(1, "Vakyarimata", "Discuție", "Discuţie");
		addNamespaceAlias(2, "Jeno", "Utilizator");
		addNamespaceAlias(3, "Jeno_vakyarimata", "Discuție_Utilizator", "Discuţie_Utilizator");
		addNamespaceAlias(5, "{{grammar:genitive-pl|Wikipedia}}_vakyarimata", "Discuție_Wikipedia", "Discuţie_Wikipedia");
		addNamespaceAlias(6, "Chitro", "Fișier", "Imagine", "Fişier");
		addNamespaceAlias(7, "Chitro_vakyarimata", "Discuție_Fișier", "Discuţie_Imagine", "Discuţie_Fişier");
		addNamespaceAlias(8, "MediyaViki", "MediaWiki");
		addNamespaceAlias(9, "MediyaViki_vakyarimata", "Discuție_MediaWiki", "Discuţie_MediaWiki");
		addNamespaceAlias(10, "Sikavno", "Format");
		addNamespaceAlias(11, "Sikavno_vakyarimata", "Discuție_Format", "Discuţie_Format");
		addNamespaceAlias(12, "Zhutipen", "Ajutor");
		addNamespaceAlias(13, "Zhutipen_vakyarimata", "Discuție_Ajutor", "Discuţie_Ajutor");
		addNamespaceAlias(14, "Shopni", "Categorie");
		addNamespaceAlias(15, "Shopni_vakyarimata", "Discuție_Categorie", "Discuţie_Categorie");

		addI18nCIAlias("redirect", "#REDIRECTEAZA", "#REDIRECT");
		addI18nCIAlias("notoc", "__FARACUPRINS__", "__NOTOC__");
		addI18nCIAlias("nogallery", "__FARAGALERIE__", "__NOGALLERY__");
		addI18nCIAlias("forcetoc", "__FORTEAZACUPRINS__", "__FORCETOC__");
		addI18nCIAlias("toc", "__CUPRINS__", "__TOC__");
		addI18nCIAlias("noeditsection", "__FARAEDITSECTIUNE__", "__NOEDITSECTION__");
		addI18nAlias("currentmonth", "NUMARLUNACURENTA", "CURRENTMONTH", "CURRENTMONTH2");
		addI18nAlias("currentmonth1", "LUNACURENTA1", "CURRENTMONTH1");
		addI18nAlias("currentmonthname", "NUMELUNACURENTA", "CURRENTMONTHNAME");
		addI18nAlias("currentmonthnamegen", "NUMELUNACURENTAGEN", "CURRENTMONTHNAMEGEN");
		addI18nAlias("currentmonthabbrev", "LUNACURENTAABREV", "CURRENTMONTHABBREV");
		addI18nAlias("currentday", "NUMARZIUACURENTA", "CURRENTDAY");
		addI18nAlias("currentday2", "NUMARZIUACURENTA2", "CURRENTDAY2");
		addI18nAlias("currentdayname", "NUMEZIUACURENTA", "CURRENTDAYNAME");
		addI18nAlias("currentyear", "ANULCURENT", "CURRENTYEAR");
		addI18nAlias("currenttime", "TIMPULCURENT", "CURRENTTIME");
		addI18nAlias("currenthour", "ORACURENTA", "CURRENTHOUR");
		addI18nAlias("localmonth", "LUNALOCALA", "LUNALOCALA2", "LOCALMONTH", "LOCALMONTH2");
		addI18nAlias("localmonth1", "LUNALOCALA1", "LOCALMONTH1");
		addI18nAlias("localmonthname", "NUMELUNALOCALA", "LOCALMONTHNAME");
		addI18nAlias("localmonthnamegen", "NUMELUNALOCALAGEN", "LOCALMONTHNAMEGEN");
		addI18nAlias("localmonthabbrev", "LUNALOCALAABREV", "LOCALMONTHABBREV");
		addI18nAlias("localday", "ZIUALOCALA", "LOCALDAY");
		addI18nAlias("localday2", "ZIUALOCALA2", "LOCALDAY2");
		addI18nAlias("localdayname", "NUMEZIUALOCALA", "LOCALDAYNAME");
		addI18nAlias("localyear", "ANULLOCAL", "LOCALYEAR");
		addI18nAlias("localtime", "TIMPULLOCAL", "LOCALTIME");
		addI18nAlias("localhour", "ORALOCALA", "LOCALHOUR");
		addI18nAlias("numberofpages", "NUMARDEPAGINI", "NUMBEROFPAGES");
		addI18nAlias("numberofarticles", "NUMARDEARTICOLE", "NUMBEROFARTICLES");
		addI18nAlias("numberoffiles", "NUMARDEFISIERE", "NUMBEROFFILES");
		addI18nAlias("numberofusers", "NUMARDEUTILIZATORI", "NUMBEROFUSERS");
		addI18nAlias("numberofactiveusers", "NUMARDEUTILIZATORIACTIVI", "NUMBEROFACTIVEUSERS");
		addI18nAlias("numberofedits", "NUMARDEMODIFICARI", "NUMBEROFEDITS");
		addI18nAlias("numberofviews", "NUMARDEVIZUALIZARI", "NUMBEROFVIEWS");
		addI18nAlias("pagename", "NUMEPAGINA", "PAGENAME");
		addI18nAlias("pagenamee", "NUMEEPAGINA", "PAGENAMEE");
		addI18nAlias("namespace", "SPATIUDENUME", "NAMESPACE");
		addI18nAlias("namespacee", "SPATIUUDENUME", "NAMESPACEE");
		addI18nAlias("talkspace", "SPATIUDEDISCUTIE", "TALKSPACE");
		addI18nAlias("talkspacee", "SPATIUUDEDISCUTIE", "TALKSPACEE");
		addI18nAlias("subjectspace", "SPATIUSUBIECT", "SPATIUARTICOL", "SUBJECTSPACE", "ARTICLESPACE");
		addI18nAlias("subjectspacee", "SPATIUUSUBIECT", "SPATIUUARTICOL", "SUBJECTSPACEE", "ARTICLESPACEE");
		addI18nAlias("fullpagename", "NUMEPAGINACOMPLET", "FULLPAGENAME");
		addI18nAlias("fullpagenamee", "NUMEEPAGINACOMPLET", "FULLPAGENAMEE");
		addI18nAlias("subpagename", "NUMESUBPAGINA", "SUBPAGENAME");
		addI18nAlias("subpagenamee", "NUMEESUBPAGINA", "SUBPAGENAMEE");
		addI18nAlias("basepagename", "NUMEDEBAZAPAGINA", "BASEPAGENAME");
		addI18nAlias("basepagenamee", "NUMEEDEBAZAPAGINA", "BASEPAGENAMEE");
		addI18nAlias("talkpagename", "NUMEPAGINADEDISCUTIE", "TALKPAGENAME");
		addI18nAlias("talkpagenamee", "NUMEEPAGINADEDISCUTIE", "TALKPAGENAMEE");
		addI18nAlias("subjectpagename", "NUMEPAGINASUBIECT", "NUMEPAGINAARTICOL", "SUBJECTPAGENAME", "ARTICLEPAGENAME");
		addI18nAlias("subjectpagenamee", "NUMEEPAGINASUBIECT", "NUMEEPAGINAARTICOL", "SUBJECTPAGENAMEE", "ARTICLEPAGENAMEE");
		addI18nCIAlias("msg", "MSJ:", "MSG:");
		addI18nCIAlias("msgnw", "MSJNOU:", "MSGNW:");
		addI18nAlias("img_thumbnail", "miniatura", "mini", "thumbnail", "thumb");
		addI18nAlias("img_manualthumb", "miniatura=$1", "mini=$1", "thumbnail=$1", "thumb=$1");
		addI18nAlias("img_right", "dreapta", "right");
		addI18nAlias("img_left", "stanga", "left");
		addI18nAlias("img_none", "nu", "none");
		addI18nAlias("img_center", "centru", "center", "centre");
		addI18nAlias("img_framed", "cadru", "framed", "enframed", "frame");
		addI18nAlias("img_frameless", "faracadru", "frameless");
		addI18nAlias("img_page", "pagina=$1", "pagina $1", "page=$1", "page $1");
		addI18nAlias("img_upright", "dreaptasus", "dreaptasus=$1", "dreaptasus $1", "upright", "upright=$1", "upright $1");
		addI18nAlias("img_border", "chenar", "border");
		addI18nAlias("img_baseline", "linia_de_bază", "baseline");
		addI18nAlias("img_sub", "indice", "sub");
		addI18nAlias("img_super", "exponent", "super", "sup");
		addI18nAlias("img_top", "sus", "top");
		addI18nAlias("img_text_top", "text-sus", "text-top");
		addI18nAlias("img_middle", "mijloc", "middle");
		addI18nAlias("img_bottom", "jos", "bottom");
		addI18nAlias("img_text_bottom", "text-jos", "text-bottom");
		addI18nAlias("img_link", "legătură=$1", "link=$1");
		addI18nAlias("sitename", "NUMESITE", "SITENAME");
		addI18nCIAlias("ns", "SN:", "NS:");
		addI18nCIAlias("localurl", "URLLOCAL:", "LOCALURL:");
		addI18nCIAlias("localurle", "URLLOCALE:", "LOCALURLE:");
		addI18nCIAlias("servername", "NUMESERVER", "SERVERNAME");
		addI18nCIAlias("scriptpath", "CALESCRIPT", "SCRIPTPATH");
		addI18nCIAlias("grammar", "GRAMATICA:", "GRAMMAR:");
		addI18nCIAlias("gender", "GEN:", "GENDER:");
		addI18nCIAlias("notitleconvert", "__FARACONVERTIRETITLU__", "__FCT__", "__NOTITLECONVERT__", "__NOTC__");
		addI18nCIAlias("nocontentconvert", "__FARACONVERTIRECONTINUT__", "__FCC__", "__NOCONTENTCONVERT__", "__NOCC__");
		addI18nAlias("currentweek", "SAPTAMANACURENTA", "CURRENTWEEK");
		addI18nAlias("localweek", "SAPTAMANALOCALA", "LOCALWEEK");
		addI18nAlias("revisionid", "IDREVIZIE", "REVISIONID");
		addI18nAlias("revisionday", "ZIREVIZIE", "REVISIONDAY");
		addI18nAlias("revisionday2", "ZIREVIZIE2", "REVISIONDAY2");
		addI18nAlias("revisionmonth", "LUNAREVIZIE", "REVISIONMONTH");
		addI18nAlias("revisionyear", "ANREVIZIE", "REVISIONYEAR");
		addI18nAlias("revisiontimestamp", "STAMPILATIMPREVIZIE", "REVISIONTIMESTAMP");
		addI18nAlias("revisionuser", "UTILIZATORREVIZIE", "REVISIONUSER");
		addI18nCIAlias("fullurl", "URLCOMPLET:", "FULLURL:");
		addI18nCIAlias("fullurle", "URLCOMPLETE:", "FULLURLE:");
		addI18nCIAlias("lcfirst", "MINUSCULAPRIMA:", "LCFIRST:");
		addI18nCIAlias("ucfirst", "MAJUSCULAPRIMA:", "UCFIRST:");
		addI18nCIAlias("lc", "MINUSCULA:", "LC:");
		addI18nCIAlias("uc", "MAJUSCULA:", "UC:");
		addI18nCIAlias("raw", "BRUT:", "RAW:");
		addI18nAlias("displaytitle", "ARATATITLU", "DISPLAYTITLE");
		addI18nAlias("newsectionlink", "__LEGATURASECTIUNENOUA__", "__NEWSECTIONLINK__");
		addI18nAlias("nonewsectionlink", "__FARALEGATURASECTIUNENOUA__", "__NONEWSECTIONLINK__");
		addI18nAlias("currentversion", "VERSIUNECURENTA", "CURRENTVERSION");
		addI18nCIAlias("urlencode", "CODIFICAREURL:", "URLENCODE:");
		addI18nCIAlias("anchorencode", "CODIFICAREANCORA", "ANCHORENCODE");
		addI18nAlias("currenttimestamp", "STAMPILATIMPCURENT", "CURRENTTIMESTAMP");
		addI18nAlias("localtimestamp", "STAMPILATIMPLOCAL", "LOCALTIMESTAMP");
		addI18nAlias("directionmark", "SEMNDIRECTIE", "DIRECTIONMARK", "DIRMARK");
		addI18nCIAlias("language", "#LIMBA:", "#LANGUAGE:");
		addI18nAlias("contentlanguage", "LIMBACONTINUT", "CONTENTLANGUAGE", "CONTENTLANG");
		addI18nAlias("pagesinnamespace", "PANIGIINSPATIULDENUME:", "PAGINIINSN:", "PAGESINNAMESPACE:", "PAGESINNS:");
		addI18nAlias("numberofadmins", "NUMARADMINI", "NUMBEROFADMINS");
		addI18nCIAlias("formatnum", "FORMATNR", "FORMATNUM");
		addI18nAlias("defaultsort", "SORTAREIMPLICITA:", "CHEIESORTAREIMPLICITA:", "CATEGORIESORTAREIMPLICITA:", "DEFAULTSORT:", "DEFAULTSORTKEY:", "DEFAULTCATEGORYSORT:");
		addI18nCIAlias("filepath", "CALEAFISIERULUI:", "FILEPATH:");
		addI18nCIAlias("tag", "eticheta", "tag");
		addI18nAlias("hiddencat", "__ASCUNDECAT__", "__HIDDENCAT__");
		addI18nAlias("pagesincategory", "PAGINIINCATEGORIE", "PAGINIINCAT", "PAGESINCATEGORY", "PAGESINCAT");
		addI18nAlias("pagesize", "MARIMEPAGINA", "PAGESIZE");
		addI18nAlias("noindex", "__FARAINDEX__", "__NOINDEX__");
		addI18nAlias("numberingroup", "NUMARINGRUP", "NUMINGRUP", "NUMBERINGROUP", "NUMINGROUP");
		addI18nAlias("staticredirect", "__REDIRECTIONARESTATICA__", "__STATICREDIRECT__");
		addI18nAlias("protectionlevel", "NIVELPROTECTIE", "PROTECTIONLEVEL");
		addI18nCIAlias("formatdate", "formatdata", "dataformat", "formatdate", "dateformat");
	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://rmy.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "rmy";
	}
}
