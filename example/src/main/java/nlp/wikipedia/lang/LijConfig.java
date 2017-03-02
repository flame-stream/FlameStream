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

public class LijConfig extends TemplateConfig {
	public LijConfig() {
		addNamespaceAlias(-2, "Media", "Media");
		addNamespaceAlias(-1, "Speçiale", "Speciale", "Speciale");
		addNamespaceAlias(1, "Discûscion", "Discussione", "Discussione");
		addNamespaceAlias(2, "Utente", "Utente");
		addNamespaceAlias(3, "Discûscioîn_ûtente", "Discussioni_utente", "Discussioni_utente");
		addNamespaceAlias(5, "Discûscioîn_Wikipedia", "Discussioni_Wikipedia", "Discussioni_Wikipedia");
		addNamespaceAlias(6, "Immaggine", "Immagine", "File", "Immagine");
		addNamespaceAlias(7, "Discûscioîn_immaggine", "Discussioni_immagine", "Discussioni_file", "Discussioni_immagine");
		addNamespaceAlias(9, "Discûscioîn_MediaWiki", "Discussioni_MediaWiki", "Discussioni_MediaWiki");
		addNamespaceAlias(11, "Discûscioîn_template", "Discussioni_template", "Discussioni_template");
		addNamespaceAlias(12, "Agiûtto", "Aiuto", "Aiuto");
		addNamespaceAlias(13, "Discûscioîn_agiûtto", "Discussioni_aiuto", "Discussioni_aiuto");
		addNamespaceAlias(14, "Categorîa", "Categoria", "Categoria");
		addNamespaceAlias(15, "Discûscioîn_categorîa", "Discussioni_categoria", "Discussioni_categoria");
		addNamespaceAlias(8, "MediaWiki");
		addNamespaceAlias(10, "Template");

		addI18nCIAlias("redirect", "#RINVIA", "#RINVIO", "#RIMANDO", "#REDIRECT");
		addI18nAlias("currentmonth", "MESEATTUALE", "MESECORRENTE", "CURRENTMONTH", "CURRENTMONTH2");
		addI18nAlias("currentmonthname", "NOMEMESEATTUALE", "NOMEMESECORRENTE", "CURRENTMONTHNAME");
		addI18nAlias("currentmonthnamegen", "NOMEMESEATTUALEGEN", "NOMEMESECORRENTEGEN", "CURRENTMONTHNAMEGEN");
		addI18nAlias("currentmonthabbrev", "MESEATTUALEABBREV", "MESECORRENTEABBREV", "CURRENTMONTHABBREV");
		addI18nAlias("currentday", "GIORNOATTUALE", "GIORNOCORRENTE", "CURRENTDAY");
		addI18nAlias("currentday2", "GIORNOATTUALE2", "GIORNOCORRENTE2", "CURRENTDAY2");
		addI18nAlias("currentdayname", "NOMEGIORNOATTUALE", "NOMEGIORNOCORRENTE", "CURRENTDAYNAME");
		addI18nAlias("currentyear", "ANNOATTUALE", "ANNOCORRENTE", "CURRENTYEAR");
		addI18nAlias("currenttime", "ORARIOATTUALE", "CURRENTTIME");
		addI18nAlias("currenthour", "ORAATTUALE", "ORACORRENTE", "CURRENTHOUR");
		addI18nAlias("localmonth", "MESELOCALE", "MESELOCALE2", "LOCALMONTH", "LOCALMONTH2");
		addI18nAlias("localmonth1", "MESELOCALE1", "LOCALMONTH1");
		addI18nAlias("localmonthname", "NOMEMESELOCALE", "LOCALMONTHNAME");
		addI18nAlias("localmonthnamegen", "NOMEMESELOCALEGEN", "LOCALMONTHNAMEGEN");
		addI18nAlias("localmonthabbrev", "MESELOCALEABBREV", "LOCALMONTHABBREV");
		addI18nAlias("localday", "GIORNOLOCALE", "LOCALDAY");
		addI18nAlias("localday2", "GIORNOLOCALE2", "LOCALDAY2");
		addI18nAlias("localdayname", "NOMEGIORNOLOCALE", "LOCALDAYNAME");
		addI18nAlias("localyear", "ANNOLOCALE", "LOCALYEAR");
		addI18nAlias("localtime", "ORARIOLOCALE", "LOCALTIME");
		addI18nAlias("localhour", "ORALOCALE", "LOCALHOUR");
		addI18nAlias("numberofpages", "NUMEROPAGINE", "NUMBEROFPAGES");
		addI18nAlias("numberofarticles", "NUMEROVOCI", "NUMEROARTICOLI", "NUMBEROFARTICLES");
		addI18nAlias("numberoffiles", "NUMEROFILE", "NUMBEROFFILES");
		addI18nAlias("numberofusers", "NUMEROUTENTI", "NUMBEROFUSERS");
		addI18nAlias("numberofactiveusers", "NUMEROUTENTIATTIVI", "NUMBEROFACTIVEUSERS");
		addI18nAlias("numberofedits", "NUMEROMODIFICHE", "NUMEROEDIT", "NUMBEROFEDITS");
		addI18nAlias("numberofviews", "NUMEROVISITE", "NUMBEROFVIEWS");
		addI18nAlias("pagename", "TITOLOPAGINA", "PAGENAME");
		addI18nAlias("pagenamee", "TITOLOPAGINAE", "PAGENAMEE");
		addI18nAlias("subpagename", "NOMESOTTOPAGINA", "SUBPAGENAME");
		addI18nAlias("subpagenamee", "NOMESOTTOPAGINAE", "SUBPAGENAMEE");
		addI18nCIAlias("subst", "SOST:", "SUBST:");
		addI18nAlias("img_thumbnail", "miniatura", "min", "thumbnail", "thumb");
		addI18nAlias("img_manualthumb", "miniatura=$1", "min=$1", "thumbnail=$1", "thumb=$1");
		addI18nAlias("img_right", "destra", "right");
		addI18nAlias("img_left", "sinistra", "left");
		addI18nAlias("img_none", "nessuno", "none");
		addI18nAlias("img_center", "centro", "center", "centre");
		addI18nAlias("img_framed", "riquadrato", "incorniciato", "originale", "framed", "enframed", "frame");
		addI18nAlias("img_frameless", "senza_cornice", "frameless");
		addI18nAlias("img_page", "pagina=$1", "pagina_$1", "page=$1", "page $1");
		addI18nAlias("img_upright", "verticale", "verticale=$1", "verticale_$1", "upright", "upright=$1", "upright $1");
		addI18nAlias("img_border", "bordo", "border");
		addI18nAlias("img_sub", "pedice", "sub");
		addI18nAlias("img_top", "sopra", "top");
		addI18nAlias("img_text_top", "testo-sopra", "text-top");
		addI18nAlias("img_middle", "metà", "middle");
		addI18nAlias("img_bottom", "sotto", "bottom");
		addI18nAlias("img_text_bottom", "testo-sotto", "text-bottom");
		addI18nAlias("sitename", "NOMESITO", "SITENAME");
		addI18nCIAlias("servername", "NOMESERVER", "SERVERNAME");
		addI18nCIAlias("gender", "GENERE:", "GENDER:");
		addI18nAlias("currentweek", "SETTIMANACORRENTE", "CURRENTWEEK");
		addI18nAlias("localweek", "SETTIMANALOCALE", "LOCALWEEK");
		addI18nCIAlias("plural", "PLURALE:", "PLURAL:");
		addI18nAlias("displaytitle", "MOSTRATITOLO", "DISPLAYTITLE");
		addI18nCIAlias("language", "#LINGUA", "#LANGUAGE:");
		addI18nAlias("numberofadmins", "NUMEROADMIN", "NUMBEROFADMINS");
		addI18nCIAlias("special", "speciale", "special");
		addI18nCIAlias("tag", "etichetta", "tag");
		addI18nAlias("pagesincategory", "PAGINEINCAT", "PAGESINCATEGORY", "PAGESINCAT");
		addI18nAlias("pagesize", "DIMENSIONEPAGINA", "PESOPAGINA", "PAGESIZE");
		addI18nAlias("index", "__INDICE__", "__INDEX__");
		addI18nAlias("noindex", "__NOINDICE__", "__NOINDEX__");
		addI18nAlias("protectionlevel", "LIVELLOPROTEZIONE", "PROTECTIONLEVEL");
		addI18nCIAlias("formatdate", "formatodata", "formatdate", "dateformat");
		addI18nCIAlias("pagesincategory_pages", "pagine", "pages");
		addI18nCIAlias("pagesincategory_files", "file", "files");
	}

	@Override
	protected String getSiteName() {
		return "Wikipedia";
	}

	@Override
	protected String getWikiUrl() {
		return "http://lij.wikipedia.org/";
	}

	@Override
	public String getIso639() {
		return "lij";
	}
}
