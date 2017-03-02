package nlp.wikipedia.lang;

/**
 * Created by marcus on 2015-03-23.
 */
public class GermanConfig extends TemplateConfig {
    public GermanConfig() {
        addNamespaceAlias(-2, "Medium");
        addNamespaceAlias(-1, "Spezial");
        addNamespaceAlias(1, "Diskussion");
        addNamespaceAlias(2, "Benutzer", "Benutzer", "Benutzerin");
        addNamespaceAlias(3, "Benutzer_Diskussion", "Benutzer_Diskussion", "Benutzerin_Diskussion");
        addNamespaceAlias(4, "Wikipedia");
        addNamespaceAlias(5, "Wikipedia_Diskussion");
        addNamespaceAlias(6, "Datei", "Bild");
        addNamespaceAlias(7, "Datei_Diskussion", "Bild_Diskussion");
        addNamespaceAlias(8, "MediaWiki");
        addNamespaceAlias(9, "MediaWiki_Diskussion");
        addNamespaceAlias(10, "Vorlage");
        addNamespaceAlias(11, "Vorlage_Diskussion");
        addNamespaceAlias(12, "Hilfe");
        addNamespaceAlias(13, "Hilfe_Diskussion");
        addNamespaceAlias(14, "Kategorie");
        addNamespaceAlias(15, "Kategorie_Diskussion");

        addI18nCIAlias("redirect", "#WEITERLEITUNG", "#REDIRECT");
        addI18nCIAlias("notoc", "__KEIN_INHALTSVERZEICHNIS__", "__KEININHALTSVERZEICHNIS__", "__NOTOC__");
        addI18nCIAlias("nogallery", "__KEINE_GALERIE__", "__KEINEGALERIE__", "__NOGALLERY__");
        addI18nCIAlias("forcetoc", "__INHALTSVERZEICHNIS_ERZWINGEN__", "__FORCETOC__");
        addI18nCIAlias("toc", "__INHALTSVERZEICHNIS__", "__TOC__");
        addI18nCIAlias("noeditsection", "__ABSCHNITTE_NICHT_BEARBEITEN__", "__NOEDITSECTION__");
        addI18nAlias("currentmonth", "JETZIGER_MONAT", "JETZIGER_MONAT_2", "CURRENTMONTH", "CURRENTMONTH2");
        addI18nAlias("currentmonth1", "JETZIGER_MONAT_1", "CURRENTMONTH1");
        addI18nAlias("currentmonthname", "JETZIGER_MONATSNAME", "CURRENTMONTHNAME");
        addI18nAlias("currentmonthnamegen", "JETZIGER_MONATSNAME_GENITIV", "JETZIGER_MONATSNAME_GEN", "CURRENTMONTHNAMEGEN");
        addI18nAlias("currentmonthabbrev", "JETZIGER_MONATSNAME_KURZ", "CURRENTMONTHABBREV");
        addI18nAlias("currentday", "JETZIGER_KALENDERTAG", "JETZIGER_TAG", "CURRENTDAY");
        addI18nAlias("currentday2", "JETZIGER_KALENDERTAG_2", "JETZIGER_TAG_2", "CURRENTDAY2");
        addI18nAlias("currentdayname", "JETZIGER_WOCHENTAG", "CURRENTDAYNAME");
        addI18nAlias("currentyear", "JETZIGES_JAHR", "CURRENTYEAR");
        addI18nAlias("currenttime", "JETZIGE_UHRZEIT", "CURRENTTIME");
        addI18nAlias("currenthour", "JETZIGE_STUNDE", "CURRENTHOUR");
        addI18nAlias("localmonth", "LOKALER_MONAT", "LOKALER_MONAT_2", "LOCALMONTH", "LOCALMONTH2");
        addI18nAlias("localmonth1", "LOKALER_MONAT_1", "LOCALMONTH1");
        addI18nAlias("localmonthname", "LOKALER_MONATSNAME", "LOCALMONTHNAME");
        addI18nAlias("localmonthnamegen", "LOKALER_MONATSNAME_GENITIV", "LOKALER_MONATSNAME_GEN", "LOCALMONTHNAMEGEN");
        addI18nAlias("localmonthabbrev", "LOKALER_MONATSNAME_KURZ", "LOCALMONTHABBREV");
        addI18nAlias("localday", "LOKALER_KALENDERTAG", "LOKALER_TAG", "LOCALDAY");
        addI18nAlias("localday2", "LOKALER_KALENDERTAG_2", "LOKALER_TAG_2", "LOCALDAY2");
        addI18nAlias("localdayname", "LOKALER_WOCHENTAG", "LOCALDAYNAME");
        addI18nAlias("localyear", "LOKALES_JAHR", "LOCALYEAR");
        addI18nAlias("localtime", "LOKALE_UHRZEIT", "LOCALTIME");
        addI18nAlias("localhour", "LOKALE_STUNDE", "LOCALHOUR");
        addI18nAlias("numberofpages", "SEITENANZAHL", "NUMBEROFPAGES");
        addI18nAlias("numberofarticles", "ARTIKELANZAHL", "NUMBEROFARTICLES");
        addI18nAlias("numberoffiles", "DATEIANZAHL", "NUMBEROFFILES");
        addI18nAlias("numberofusers", "BENUTZERANZAHL", "NUMBEROFUSERS");
        addI18nAlias("numberofactiveusers", "AKTIVE_BENUTZER", "NUMBEROFACTIVEUSERS");
        addI18nAlias("numberofedits", "BEARBEITUNGSANZAHL", "NUMBEROFEDITS");
        addI18nAlias("numberofviews", "BETRACHTUNGEN", "NUMBEROFVIEWS");
        addI18nAlias("pagename", "SEITENNAME", "PAGENAME");
        addI18nAlias("pagenamee", "SEITENNAME_URL", "PAGENAMEE");
        addI18nAlias("namespace", "NAMENSRAUM", "NAMESPACE");
        addI18nAlias("namespacee", "NAMENSRAUM_URL", "NAMESPACEE");
        addI18nAlias("namespacenumber", "NAMENSRAUMNUMMER", "NAMESPACENUMBER");
        addI18nAlias("talkspace", "DISKUSSIONSNAMENSRAUM", "DISK_NR", "TALKSPACE");
        addI18nAlias("talkspacee", "DISKUSSIONSNAMENSRAUM_URL", "DISK_NR_URL", "TALKSPACEE");
        addI18nAlias("subjectspace", "HAUPTNAMENSRAUM", "SUBJECTSPACE", "ARTICLESPACE");
        addI18nAlias("subjectspacee", "HAUPTNAMENSRAUM_URL", "SUBJECTSPACEE", "ARTICLESPACEE");
        addI18nAlias("fullpagename", "VOLLER_SEITENNAME", "FULLPAGENAME");
        addI18nAlias("fullpagenamee", "VOLLER_SEITENNAME_URL", "FULLPAGENAMEE");
        addI18nAlias("subpagename", "UNTERSEITE", "SUBPAGENAME");
        addI18nAlias("subpagenamee", "UNTERSEITE_URL", "SUBPAGENAMEE");
        addI18nAlias("rootpagename", "STAMMSEITE", "ROOTPAGENAME");
        addI18nAlias("rootpagenamee", "STAMMSEITE_URL", "ROOTPAGENAMEE");
        addI18nAlias("basepagename", "OBERSEITE", "BASEPAGENAME");
        addI18nAlias("basepagenamee", "OBERSEITE_URL", "BASEPAGENAMEE");
        addI18nAlias("talkpagename", "DISKUSSIONSSEITE", "DISK", "TALKPAGENAME");
        addI18nAlias("talkpagenamee", "DISKUSSIONSSEITE_URL", "DISK_URL", "TALKPAGENAMEE");
        addI18nAlias("subjectpagename", "HAUPTSEITENNAME", "VORDERSEITE", "HAUPTSEITE", "SUBJECTPAGENAME", "ARTICLEPAGENAME");
        addI18nAlias("subjectpagenamee", "HAUPTSEITENNAME_URL", "VORDERSEITE_URL", "HAUPTSEITE_URL", "SUBJECTPAGENAMEE", "ARTICLEPAGENAMEE");
        addI18nCIAlias("subst", "ERS:", "SUBST:");
        addI18nCIAlias("safesubst", "SICHER_ERS:", "SICHERERS:", "SAFESUBST:");
        addI18nAlias("img_thumbnail", "mini", "miniatur", "thumbnail", "thumb");
        addI18nAlias("img_manualthumb", "miniatur=$1", "mini=$1", "thumbnail=$1", "thumb=$1");
        addI18nAlias("img_right", "rechts", "right");
        addI18nAlias("img_left", "links", "left");
        addI18nAlias("img_none", "ohne", "none");
        addI18nAlias("img_center", "zentriert", "center", "centre");
        addI18nAlias("img_framed", "gerahmt", "framed", "enframed", "frame");
        addI18nAlias("img_frameless", "rahmenlos", "frameless");
        addI18nAlias("img_lang", "sprache=$1", "lang=$1");
        addI18nAlias("img_page", "seite=$1", "seite_$1", "page=$1", "page $1");
        addI18nAlias("img_upright", "hochkant", "hochkant=$1", "hochkant_$1", "upright", "upright=$1", "upright $1");
        addI18nAlias("img_border", "rand", "border");
        addI18nAlias("img_baseline", "grundlinie", "baseline");
        addI18nAlias("img_sub", "tiefgestellt", "tief", "sub");
        addI18nAlias("img_super", "hochgestellt", "hoch", "super", "sup");
        addI18nAlias("img_top", "oben", "top");
        addI18nAlias("img_text_top", "text-oben", "text-top");
        addI18nAlias("img_middle", "mitte", "middle");
        addI18nAlias("img_bottom", "unten", "bottom");
        addI18nAlias("img_text_bottom", "text-unten", "text-bottom");
        addI18nAlias("img_link", "verweis=$1", "link=$1");
        addI18nAlias("img_alt", "alternativtext=$1", "alt=$1");
        addI18nAlias("img_class", "klasse=$1", "class=$1");
        addI18nCIAlias("int", "NACHRICHT:", "INT:");
        addI18nAlias("sitename", "PROJEKTNAME", "SITENAME");
        addI18nCIAlias("ns", "NR:", "NS:");
        addI18nCIAlias("nse", "NR_URL:", "NSE:");
        addI18nCIAlias("localurl", "LOKALE_URL:", "LOCALURL:");
        addI18nCIAlias("localurle", "LOKALE_URL_C:", "LOCALURLE:");
        addI18nCIAlias("articlepath", "ARTIKELPFAD", "ARTICLEPATH");
        addI18nCIAlias("pageid", "SEITENID", "SEITENKENNUNG", "PAGEID");
        addI18nCIAlias("scriptpath", "SKRIPTPFAD", "SCRIPTPATH");
        addI18nCIAlias("stylepath", "STILPFAD", "STYLEPFAD", "STYLEPATH");
        addI18nCIAlias("grammar", "GRAMMATIK:", "GRAMMAR:");
        addI18nCIAlias("gender", "GESCHLECHT:", "GENDER:");
        addI18nCIAlias("notitleconvert", "__KEINE_TITELKONVERTIERUNG__", "__NOTITLECONVERT__", "__NOTC__");
        addI18nCIAlias("nocontentconvert", "__KEINE_INHALTSKONVERTIERUNG__", "__NOCONTENTCONVERT__", "__NOCC__");
        addI18nAlias("currentweek", "JETZIGE_KALENDERWOCHE", "JETZIGE_WOCHE", "CURRENTWEEK");
        addI18nAlias("currentdow", "JETZIGER_WOCHENTAG_ZAHL", "CURRENTDOW");
        addI18nAlias("localweek", "LOKALE_KALENDERWOCHE", "LOKALE_WOCHE", "LOCALWEEK");
        addI18nAlias("localdow", "LOKALER_WOCHENTAG_ZAHL", "LOCALDOW");
        addI18nAlias("revisionid", "REVISIONSID", "VERSIONSID", "REVISIONID");
        addI18nAlias("revisionday", "REVISIONSTAG", "VERSIONSTAG", "REVISIONDAY");
        addI18nAlias("revisionday2", "REVISIONSTAG2", "VERSIONSTAG2", "REVISIONDAY2");
        addI18nAlias("revisionmonth", "REVISIONSMONAT", "VERSIONSMONAT", "REVISIONMONTH");
        addI18nAlias("revisionmonth1", "REVISIONSMONAT1", "VERSIONSMONAT1", "REVISIONMONTH1");
        addI18nAlias("revisionyear", "REVISIONSJAHR", "VERSIONSJAHR", "REVISIONYEAR");
        addI18nAlias("revisiontimestamp", "REVISIONSZEITSTEMPEL", "VERSIONSZEITSTEMPEL", "REVISIONTIMESTAMP");
        addI18nAlias("revisionuser", "REVISIONSBENUTZER", "VERSIONSBENUTZER", "REVISIONUSER");
        addI18nAlias("revisionsize", "VERSIONSGRÖSSE", "REVISIONSIZE");
        addI18nCIAlias("fullurl", "VOLLSTÄNDIGE_URL:", "FULLURL:");
        addI18nCIAlias("fullurle", "VOLLSTÄNDIGE_URL_C:", "FULLURLE:");
        addI18nCIAlias("canonicalurl", "KANONISCHE_URL:", "CANONICALURL:");
        addI18nCIAlias("canonicalurle", "KANONISCHE_URL_C:", "CANONICALURLE:");
        addI18nCIAlias("lcfirst", "INITIAL_KLEIN:", "LCFIRST:");
        addI18nCIAlias("ucfirst", "INITIAL_GROSS:", "UCFIRST:");
        addI18nCIAlias("lc", "KLEIN:", "LC:");
        addI18nCIAlias("uc", "GROSS:", "UC:");
        addI18nCIAlias("raw", "ROH:", "RAW:");
        addI18nAlias("displaytitle", "SEITENTITEL", "DISPLAYTITLE");
        addI18nAlias("newsectionlink", "__NEUER_ABSCHNITTSLINK__", "__PLUS_LINK__", "__NEWSECTIONLINK__");
        addI18nAlias("nonewsectionlink", "__KEIN_NEUER_ABSCHNITTSLINK__", "__KEIN_PLUS_LINK__", "__NONEWSECTIONLINK__");
        addI18nAlias("currentversion", "JETZIGE_VERSION", "CURRENTVERSION");
        addI18nCIAlias("urlencode", "URLENKODIERT:", "URLENCODE:");
        addI18nCIAlias("anchorencode", "ANKERENKODIERT:", "SPRUNGMARKEENKODIERT:", "ANCHORENCODE");
        addI18nAlias("currenttimestamp", "JETZIGER_ZEITSTEMPEL", "CURRENTTIMESTAMP");
        addI18nAlias("localtimestamp", "LOKALER_ZEITSTEMPEL", "LOCALTIMESTAMP");
        addI18nAlias("directionmark", "TEXTAUSRICHTUNG", "DIRECTIONMARK", "DIRMARK");
        addI18nCIAlias("language", "#SPRACHE:", "#LANGUAGE:");
        addI18nAlias("contentlanguage", "INHALTSSPRACHE", "CONTENTLANGUAGE", "CONTENTLANG");
        addI18nAlias("pagesinnamespace", "SEITEN_IM_NAMENSRAUM:", "SEITEN_IN_NR:", "SEITEN_NR:", "PAGESINNAMESPACE:", "PAGESINNS:");
        addI18nAlias("numberofadmins", "ADMINANZAHL", "NUMBEROFADMINS");
        addI18nCIAlias("formatnum", "ZAHLENFORMAT", "FORMATNUM");
        addI18nCIAlias("padleft", "FÜLLENLINKS", "PADLEFT");
        addI18nCIAlias("padright", "FÜLLENRECHTS", "PADRIGHT");
        addI18nCIAlias("special", "spezial", "special");
        addI18nCIAlias("speciale", "speziale", "speciale");
        addI18nAlias("defaultsort", "SORTIERUNG:", "DEFAULTSORT:", "DEFAULTSORTKEY:", "DEFAULTCATEGORYSORT:");
        addI18nCIAlias("filepath", "DATEIPFAD:", "FILEPATH:");
        addI18nCIAlias("tag", "erweiterung", "tag");
        addI18nAlias("hiddencat", "__VERSTECKTE_KATEGORIE__", "__WARTUNGSKATEGORIE__", "__HIDDENCAT__");
        addI18nAlias("pagesincategory", "SEITEN_IN_KATEGORIE", "SEITEN_KAT", "SEITENINKAT", "PAGESINCATEGORY", "PAGESINCAT");
        addI18nAlias("pagesize", "SEITENGRÖSSE", "PAGESIZE");
        addI18nAlias("index", "__INDEXIEREN__", "__INDIZIEREN__", "__INDEX__");
        addI18nAlias("noindex", "__NICHT_INDEXIEREN__", "__KEIN_INDEX__", "__NICHT_INDIZIEREN__", "__NOINDEX__");
        addI18nAlias("numberingroup", "BENUTZER_IN_GRUPPE", "NUMBERINGROUP", "NUMINGROUP");
        addI18nAlias("staticredirect", "__PERMANENTE_WEITERLEITUNG__", "__STATICREDIRECT__");
        addI18nAlias("protectionlevel", "SCHUTZSTATUS", "PROTECTIONLEVEL");
        addI18nCIAlias("formatdate", "DATUMSFORMAT", "formatdate", "dateformat");
        addI18nCIAlias("url_path", "PFAD", "PATH");
        addI18nCIAlias("url_query", "ABFRAGE", "QUERY");
        addI18nCIAlias("defaultsort_noerror", "keinfehler", "noerror");
        addI18nCIAlias("defaultsort_noreplace", "keineersetzung", "noreplace");
        addI18nCIAlias("pagesincategory_all", "alle", "all");
        addI18nCIAlias("pagesincategory_pages", "seiten", "pages");
        addI18nCIAlias("pagesincategory_subcats", "unterkategorien", "unterkats", "subcats");
        addI18nCIAlias("pagesincategory_files", "dateien", "files");
    }

    @Override
    protected String getSiteName() {
        return "Wikipedia";
    }

    @Override
    protected String getWikiUrl() {
        return "http://de.wikipedia.org/";
    }

    @Override
    public String getIso639() {
        return "de";
    }
}
