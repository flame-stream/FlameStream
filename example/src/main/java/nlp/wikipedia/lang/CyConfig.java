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

public class CyConfig extends TemplateConfig {
	public CyConfig() {
		addNamespaceAlias(-2, "Media");
		addNamespaceAlias(-1, "Arbennig");
		addNamespaceAlias(1, "Sgwrs");
		addNamespaceAlias(2, "Defnyddiwr");
		addNamespaceAlias(3, "Sgwrs_Defnyddiwr");
		addNamespaceAlias(5, "Sgwrs_Wikipedia");
		addNamespaceAlias(6, "Delwedd");
		addNamespaceAlias(7, "Sgwrs_Delwedd");
		addNamespaceAlias(8, "MediaWici");
		addNamespaceAlias(9, "Sgwrs_MediaWici");
		addNamespaceAlias(10, "Nodyn");
		addNamespaceAlias(11, "Sgwrs_Nodyn");
		addNamespaceAlias(12, "Cymorth");
		addNamespaceAlias(13, "Sgwrs_Cymorth");
		addNamespaceAlias(14, "Categori");
		addNamespaceAlias(15, "Sgwrs_Categori");

		addI18nCIAlias("redirect", "#ail-cyfeirio", "#ailgyfeirio", "#REDIRECT");
		addI18nCIAlias("notoc", "__DIMTAFLENCYNNWYS__", "__DIMRHESTRGYNNWYS__", "__DIMRHG__", "__NOTOC__");
		addI18nCIAlias("noeditsection", "__DIMADRANGOLYGU__", "__DIMGOLYGUADRAN__", "__NOEDITSECTION__");
		addI18nAlias("currentmonth", "MISCYFOES", "MISCYFREDOL", "CURRENTMONTH", "CURRENTMONTH2");
		addI18nAlias("currentmonthname", "ENWMISCYFOES", "ENWMISCYFREDOL", "CURRENTMONTHNAME");
		addI18nAlias("currentmonthnamegen", "GENENWMISCYFOES", "CURRENTMONTHNAMEGEN");
		addI18nAlias("currentday", "DYDDIADCYFOES", "DYDDCYFREDOL", "CURRENTDAY");
		addI18nAlias("currentdayname", "ENWDYDDCYFOES", "ENWDYDDCYFREDOL", "CURRENTDAYNAME");
		addI18nAlias("currentyear", "FLWYDDYNCYFOES", "BLWYDDYNGYFREDOL", "CURRENTYEAR");
		addI18nAlias("currenttime", "AMSERCYFOES", "AMSERCYFREDOL", "CURRENTTIME");
		addI18nAlias("currenthour", "AWRGYFREDOL", "CURRENTHOUR");
		addI18nAlias("numberofarticles", "NIFEROERTHYGLAU", "NIFERYRERTHYGLAU", "NUMBEROFARTICLES");
		addI18nAlias("numberoffiles", "NIFERYFFEILIAU", "NUMBEROFFILES");
		addI18nAlias("numberofusers", "NIFERYDEFNYDDWYR", "NUMBEROFUSERS");
		addI18nAlias("numberofedits", "NIFERYGOLYGIADAU", "NUMBEROFEDITS");
		addI18nAlias("pagename", "ENWTUDALEN", "PAGENAME");
		addI18nAlias("pagenamee", "ENWTUDALENE", "PAGENAMEE");
		addI18nAlias("namespace", "PARTH", "NAMESPACE");
		addI18nAlias("namespacee", "NAMESPACE", "PARTHE", "NAMESPACEE");
		addI18nAlias("fullpagename", "ENWLLAWNTUDALEN", "FULLPAGENAME");
		addI18nAlias("fullpagenamee", "ENWLLAWNTUDALENE", "FULLPAGENAMEE");
		addI18nAlias("subpagename", "ENWISDUDALEN", "SUBPAGENAME");
		addI18nAlias("subpagenamee", "ENWISDUDALENE", "SUBPAGENAMEE");
		addI18nAlias("talkpagename", "ENWTUDALENSGWRS", "TALKPAGENAME");
		addI18nAlias("talkpagenamee", "ENWTUDALENSGWRSE", "TALKPAGENAMEE");
		addI18nAlias("img_thumbnail", "ewin_bawd", "bawd", "mân-lun", "thumbnail", "thumb");
		addI18nAlias("img_manualthumb", "mân-lun=$1", "bawd=$1", "thumbnail=$1", "thumb=$1");
		addI18nAlias("img_right", "de", "right");
		addI18nAlias("img_left", "chwith", "left");
		addI18nAlias("img_none", "dim", "none");
		addI18nAlias("img_center", "canol", "center", "centre");
		addI18nAlias("img_page", "tudalen=$1", "tudalen_$1", "page=$1", "page $1");
		addI18nAlias("img_upright", "unionsyth", "unionsyth=$1", "unionsyth_$1", "upright", "upright=$1", "upright $1");
		addI18nAlias("img_sub", "is", "sub");
		addI18nAlias("img_super", "uwch", "super", "sup");
		addI18nAlias("img_top", "brig", "top");
		addI18nAlias("img_bottom", "gwaelod", "godre", "bottom");
		addI18nCIAlias("server", "GWEINYDD", "SERVER");
		addI18nCIAlias("servername", "ENW'RGWEINYDD", "SERVERNAME");
		addI18nCIAlias("grammar", "GRAMMAR", "GRAMADEG", "GRAMMAR:");
		addI18nAlias("currentweek", "WYTHNOSGYFREDOL", "CURRENTWEEK");
		addI18nAlias("revisionid", "IDYGOLYGIAD", "REVISIONID");
		addI18nAlias("revisionday", "DIWRNODYGOLYGIAD", "REVISIONDAY");
		addI18nAlias("revisionday2", "DIWRNODYGOLYGIAD2", "REVISIONDAY2");
		addI18nAlias("revisionmonth", "MISYGOLYGIAD", "REVISIONMONTH");
		addI18nAlias("revisionyear", "BLWYDDYNYGOLYGIAD", "REVISIONYEAR");
		addI18nAlias("revisiontimestamp", "STAMPAMSERYGOLYGIAD", "REVISIONTIMESTAMP");
		addI18nCIAlias("plural", "LLUOSOG:", "PLURAL:");
		addI18nCIAlias("fullurl", "URLLLAWN:", "FULLURL:");
		addI18nCIAlias("fullurle", "URLLLAWNE:", "FULLURLE:");
		addI18nAlias("newsectionlink", "_NEWSECTIONLINK_", "_CYSWLLTADRANNEWYDD_", "__NEWSECTIONLINK__");
		addI18nAlias("currentversion", "GOLYGIADCYFREDOL", "CURRENTVERSION");
		addI18nAlias("currenttimestamp", "STAMPAMSERCYFREDOL", "CURRENTTIMESTAMP");
		addI18nAlias("localtimestamp", "STAMPAMSERLLEOL", "LOCALTIMESTAMP");
		addI18nCIAlias("language", "#IAITH:", "#LANGUAGE:");
		addI18nAlias("contentlanguage", "IAITHYCYNNWYS", "CONTENTLANGUAGE", "CONTENTLANG");
		addI18nAlias("pagesinnamespace", "TUDALENNAUYNYPARTH:", "PAGESINNAMESPACE:", "PAGESINNS:");
		addI18nAlias("numberofadmins", "NIFERYGWEINYDDWYR", "NUMBEROFADMINS");
		addI18nCIAlias("formatnum", "FFORMATIORHIF", "FORMATNUM");
		addI18nCIAlias("special", "arbennig", "special");
		addI18nAlias("hiddencat", "_HIDDENCAT_", "_CATCUDD_", "__HIDDENCAT__");
		addI18nAlias("pagesincategory", "TUDALENNAUYNYCAT", "PAGESINCATEGORY", "PAGESINCAT");
		addI18nAlias("pagesize", "MAINTTUD", "PAGESIZE");
	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://cy.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "cy";
	}
}
