param(
    [string]$WorkDir        = "$env:USERPROFILE\Downloads",
    [string]$EmailForAPI    = "pakgeniusatwork@gmail.com",
    [int]$MinWorksCount     = 5,
    [int]$MinCitedBy        = 10,
    [int]$DelayMs           = 500,
    [int]$AuthorBatch       = 50,
    [int]$BookBatch         = 30,
    [int]$EmailDelayMs      = 400
)

# Output files
$authorsCsv     = "$WorkDir\SOP_Authors_Full.csv"
$checkpointCsv  = "$WorkDir\SOP_Email_Checkpoint.csv"
$withEmailCsv   = "$WorkDir\SOP_With_Emails.csv"
$noEmailCsv     = "$WorkDir\SOP_No_Email.csv"

$amp = [char]38

# SOP subfields
$targetSubfields = @(
    [PSCustomObject]@{ Name = "Anthropology";        Id = "3314" },
    [PSCustomObject]@{ Name = "History";             Id = "1202" },
    [PSCustomObject]@{ Name = "Sociology";           Id = "3312" },
    [PSCustomObject]@{ Name = "Economics";           Id = "2002" },
    [PSCustomObject]@{ Name = "Geography";           Id = "3305" },
    [PSCustomObject]@{ Name = "Literature/English";  Id = "1208" },
    [PSCustomObject]@{ Name = "Communication/Media"; Id = "3315" },
    [PSCustomObject]@{ Name = "Cultural Studies";    Id = "3316" },
    [PSCustomObject]@{ Name = "Gender Studies";      Id = "3318" }
)
$sopSubfieldIds = ($targetSubfields | ForEach-Object { $_.Id }) -join "|"

$hardSciList = @("medicine","biology","chemistry","physics","mathematics",
                 "computer science","engineering","neuroscience","genomics",
                 "ecology","geology","machine learning","astronomy","statistics",
                 "environmental science","earth science","materials science",
                 "biochemistry","molecular","genetics","immunology","pharmacology",
                 "climate","atmospheric","oceanography","hydrology","geophysics",
                 "botany","zoology","microbiology","neurology","cardiology")

# Institutions that are clearly not universities (filter out bad OpenAlex data)
$badInstKeywords = @("twitter","facebook","google","microsoft","amazon","apple",
                     "illumina","linkedin","salesforce","oracle","ibm","intel",
                     "qualcomm","nvidia","uber","airbnb","spotify","netflix",
                     "think tank","resources for the future","rand corporation",
                     "brookings","heritage foundation","cato institute")

$tradePublishers = @(
    "penguin","random house","harpercollins","simon & schuster","hachette",
    "macmillan publishers","little, brown","little brown","grand central",
    "farrar, straus","farrar straus","pantheon","knopf","doubleday","viking",
    "riverhead","scribner","free press","atria books","w. w. norton","w.w. norton",
    "henry holt","picador","flatiron","celadon","st. martin","st martins",
    "thomas dunne","grove atlantic","grove press","soho press","algonquin",
    "counterpoint","workman","ten speed","clarkson potter","portfolio",
    "sentinel","currency books","broadway books","harmony books","three rivers",
    "anchor books","vintage books","ballantine","bantam","dell publishing",
    "delacorte","dial press","dutton","plume","signet","avon books","berkley",
    "g.p. putnam","perigee","tarcher","basic books","public affairs",
    "publicaffairs","rodale","hyperion","miramax","sourcebooks","william morrow",
    "touchstone","gallery books","pocket books","threshold editions",
    "howard books","tyndale","zondervan","thomas nelson"
)

# Force TLS 1.2 and fix connection handling
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
[Net.ServicePointManager]::Expect100Continue = $false
[Net.ServicePointManager]::DefaultConnectionLimit = 4

$apiHeaders = @{ "User-Agent" = "ScholarsToStorytellers/1.0 (mailto:$EmailForAPI)" }

function Invoke-OA([string]$Url) {
    # Different wait schedules: 429 rate-limit vs other errors
    $delay429 = @(120, 300, 600)   # 2 min, 5 min, 10 min
    $delayErr = @(5, 15, 45)       # quick retry for transient errors
    for ($i = 0; $i -lt 4; $i++) {
        try {
            $r = Invoke-RestMethod -Uri $Url -Headers $script:apiHeaders -Method Get -TimeoutSec 45 -ErrorAction Stop
            return $r
        } catch {
            $err  = $_.Exception.Message
            $is429 = $err -like "*429*" -or $err -like "*Too Many*"
            if ($i -lt 3) {
                $wait = if ($is429) { $delay429[$i] } else { $delayErr[$i] }
                $label = if ($is429) { "rate-limited (429)" } else { "error" }
                Write-Host "    [API] Attempt $($i+1) $label - waiting ${wait}s..." -ForegroundColor DarkYellow
                Start-Sleep -Seconds $wait
            } else {
                Write-Host "    [API] All retries exhausted, skipping." -ForegroundColor Red
            }
        }
    }
    return $null
}

function Split-Chunks($arr, [int]$size) {
    $chunks = [System.Collections.ArrayList]::new()
    $i = 0
    while ($i -lt $arr.Count) {
        $end = [math]::Min($i + $size - 1, $arr.Count - 1)
        [void]$chunks.Add($arr[$i..$end])
        $i += $size
    }
    return $chunks
}

# Web scraping helpers
$userAgents = @(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"
)
$uaIndex = 0

function Get-UA {
    $script:uaIndex = ($script:uaIndex + 1) % $script:userAgents.Count
    return $script:userAgents[$script:uaIndex]
}

function Fetch-Page([string]$url) {
    if (-not $url -or $url.Trim() -eq "") { return $null }
    try {
        return (Invoke-WebRequest -Uri $url -TimeoutSec 14 -UseBasicParsing -UserAgent (Get-UA) -ErrorAction Stop).Content
    } catch { return $null }
}

$skipDomains = @('sentry','example','noreply','no-reply','support','webmaster','admin',
                 'info','test','placeholder','youremail','privacy','abuse','help',
                 'feedback','contact','press','media','doi','crossref','elsevier',
                 'springer','wiley','tandfonline')
$skipExts = @('png','jpg','gif','svg','css','js','ico','woff','woff2','ttf','eot')
$emailRegex = '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'

function Get-BestEmail([string]$text, [string]$preferDomain, [string]$firstName, [string]$lastName) {
    if (-not $text) { return $null }
    $raw = [System.Collections.ArrayList]::new()
    foreach ($m in [regex]::Matches($text, $emailRegex)) {
        [void]$raw.Add(@{ email = $m.Value.Trim().ToLower(); idx = $m.Index })
    }
    foreach ($m in [regex]::Matches($text, '[a-zA-Z0-9._%+\-]+\s*[\[\(]at[\]\)]\s*[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', 'IgnoreCase')) {
        [void]$raw.Add(@{ email = ($m.Value -replace '\s*[\[\(]at[\]\)]\s*','@').ToLower(); idx = $m.Index })
    }
    $clean = $raw | Where-Object {
        $ext = ($_.email -split '\.')[-1]; $dom = ($_.email -split '@')[-1]; $loc = ($_.email -split '@')[0]
        $skipExts -notcontains $ext -and ($skipDomains | Where-Object { $dom -like "*$_*" }).Count -eq 0 -and $loc.Length -gt 1 -and $dom.Length -gt 3
    }
    if (-not $clean -or @($clean).Count -eq 0) { return $null }

    $textLower = $text.ToLower(); $lastLower = $lastName.ToLower(); $window = 600
    $nearName  = [System.Collections.ArrayList]::new()
    foreach ($c in $clean) {
        $pos = 0
        while ($true) {
            $namePos = $textLower.IndexOf($lastLower, $pos)
            if ($namePos -lt 0) { break }
            if ([math]::Abs($c.idx - $namePos) -le $window) { [void]$nearName.Add($c); break }
            $pos = $namePos + 1
        }
    }

    $useNameInLocal = $lastLower.Length -ge 4
    $nameInLocal = [System.Collections.ArrayList]::new()
    if ($useNameInLocal) {
        $last4 = $lastLower.Substring(0, [math]::Min(4, $lastLower.Length))
        foreach ($c in $clean) {
            $loc = ($c.email -split '@')[0]
            if ($loc -like "*$lastLower*" -or $loc -like "*$last4*") { [void]$nameInLocal.Add($c) }
        }
    }

    $nameInstEmail = @($nameInLocal | Where-Object { $preferDomain -and $_.email -like "*@*$preferDomain*" })
    $nameEduEmail  = @($nameInLocal | Where-Object { $_.email -like "*.edu" -or $_.email -like "*.ac.*" })
    $nameAnyEmail  = @($nameInLocal | Where-Object { $_.email -notlike "*163.com" -and $_.email -notlike "*qq.com" -and $_.email -notlike "*gmail.com" -and $_.email -notlike "*hotmail.com" -and $_.email -notlike "*yahoo.com" })
    $nearInst      = @($nearName    | Where-Object { $preferDomain -and $_.email -like "*@*$preferDomain*" })
    $nearEdu       = @($nearName    | Where-Object { $_.email -like "*.edu" -or $_.email -like "*.ac.*" })

    if ($nameInstEmail.Count -gt 0) { return @{ email = $nameInstEmail[0].email; confidence = "strict" } }
    if ($nameEduEmail.Count  -gt 0) { return @{ email = $nameEduEmail[0].email;  confidence = "strict" } }
    if ($nameAnyEmail.Count  -gt 0) { return @{ email = $nameAnyEmail[0].email;  confidence = "medium" } }
    if ($nearInst.Count      -gt 0) { return @{ email = $nearInst[0].email;      confidence = "loose-inst" } }
    if ($nearEdu.Count       -gt 0) { return @{ email = $nearEdu[0].email;       confidence = "loose-edu" } }
    return $null
}

function Name-IsOnPage([string]$content, [string]$lastName) {
    if (-not $content) { return $false }
    return $content.ToLower() -match [regex]::Escape($lastName.ToLower())
}

function Get-NameVariants([string]$fullName) {
    $parts = $fullName.Trim() -split "\s+"
    return @{ full = $fullName.Trim(); short = "$($parts[0]) $($parts[-1])"; first = $parts[0]; last = $parts[-1] }
}

function Get-AuthorPageUrl([string]$oaId) {
    $short = $oaId -replace "https://openalex.org/",""
    try {
        $r = Invoke-RestMethod -Uri "https://api.openalex.org/authors/$short" -Method Get -TimeoutSec 15 -ErrorAction Stop
        return $r.homepage_url
    } catch { return $null }
}

function Get-SopWorks([string]$oaId) {
    $short = $oaId -replace "https://openalex.org/",""
    $filter = "filter=author.id:$short,type:article,primary_topic.subfield.id:$script:sopSubfieldIds"
    $url    = "https://api.openalex.org/works?" + $filter + $amp + "sort=cited_by_count:desc" + $amp + "per_page=5" + $amp + "select=id,doi,best_oa_location,primary_location"
    try {
        $r = Invoke-RestMethod -Uri $url -Method Get -TimeoutSec 15 -ErrorAction Stop
        return $r.results
    } catch { return @() }
}

function Search-GoogleScholar([string]$fullName, [string]$shortName, [string]$institution) {
    foreach ($q in @([uri]::EscapeDataString("$fullName $institution"), [uri]::EscapeDataString("$shortName $institution"), [uri]::EscapeDataString($shortName)) | Select-Object -Unique) {
        $c = Fetch-Page "https://scholar.google.com/scholar?q=$q&hl=en"
        if ($c) { $m = [regex]::Match($c, 'href="(/citations\?user=[^"&]+)'); if ($m.Success) { return "https://scholar.google.com" + $m.Groups[1].Value } }
        Start-Sleep -Milliseconds 300
    }
    foreach ($name in @($fullName, $shortName) | Select-Object -Unique) {
        $c = Fetch-Page "https://scholar.google.com/citations?view_op=search_authors&mauthors=$([uri]::EscapeDataString($name))&hl=en"
        if ($c) { $m = [regex]::Match($c, 'href="(/citations\?user=[^"&]+)'); if ($m.Success) { return "https://scholar.google.com" + $m.Groups[1].Value } }
        Start-Sleep -Milliseconds 300
    }
    return $null
}

function Search-SemanticScholar([string]$fullName, [string]$shortName) {
    foreach ($name in @($fullName, $shortName) | Select-Object -Unique) {
        try {
            $r = Invoke-RestMethod -Uri "https://api.semanticscholar.org/graph/v1/author/search?query=$([uri]::EscapeDataString($name))&fields=name,homepage" -Method Get -TimeoutSec 15 -ErrorAction Stop
            if ($r.data -and $r.data.Count -gt 0 -and $r.data[0].homepage) { return $r.data[0].homepage }
        } catch {}
        Start-Sleep -Milliseconds 200
    }
    return $null
}

function Find-Email($row) {
    $domain    = $row.Institution_Domain -replace "https?://(www\.)?","" -replace "/.*",""
    $authorName = if ($row.Name) { $row.Name } elseif ($row.Author) { $row.Author } else { "" }
    $n      = Get-NameVariants $authorName
    $fn     = $n.first; $ln = $n.last; $full = $n.full; $short = $n.short
    $pad    = "    "
    $result = @{ email = ""; source = ""; homepage = "" }

    # 1. OpenAlex homepage
    Write-Host "$pad [1/8] OpenAlex homepage..." -ForegroundColor DarkGray
    $homepage = Get-AuthorPageUrl $row.OpenAlex_ID
    Start-Sleep -Milliseconds 250
    if ($homepage) {
        $result.homepage = $homepage
        Write-Host "$pad      -> $homepage" -ForegroundColor DarkGray
        $c = Fetch-Page $homepage
        if ($c -and (Name-IsOnPage $c $ln)) {
            $f = Get-BestEmail $c $domain $fn $ln
            if ($f -and $f.confidence -notlike "loose*") { $result.email = $f.email; $result.source = "faculty page ($($f.confidence))"; return $result }
        } elseif ($c) { Write-Host "$pad      -> name not on page, skipping" -ForegroundColor DarkYellow }
        Start-Sleep -Milliseconds 200
    } else { Write-Host "$pad      -> not in OpenAlex" -ForegroundColor DarkGray }

    # 2. SOP paper landing pages
    Write-Host "$pad [2/8] SOP article landing pages..." -ForegroundColor DarkGray
    $works = Get-SopWorks $row.OpenAlex_ID
    Start-Sleep -Milliseconds 250
    foreach ($work in $works) {
        $urls = @()
        if ($work.best_oa_location -and $work.best_oa_location.landing_page_url)  { $urls += $work.best_oa_location.landing_page_url }
        if ($work.primary_location -and $work.primary_location.landing_page_url -and $urls -notcontains $work.primary_location.landing_page_url) { $urls += $work.primary_location.landing_page_url }
        if ($work.doi -and $urls -notcontains $work.doi) { $urls += $work.doi }
        foreach ($url in $urls) {
            Write-Host "$pad      -> $url" -ForegroundColor DarkGray
            $c = Fetch-Page $url
            if (-not $c) { Start-Sleep -Milliseconds 150; continue }
            $f = Get-BestEmail $c $domain $fn $ln
            if ($f -and $f.confidence -eq "strict") { $result.email = $f.email; $result.source = "paper page ($($f.confidence))"; return $result }
            elseif ($f) { Write-Host "$pad      -> low confidence: $($f.email) [$($f.confidence)]" -ForegroundColor DarkYellow }
            Start-Sleep -Milliseconds 200
        }
    }

    # 3. Google Scholar
    Write-Host "$pad [3/8] Google Scholar..." -ForegroundColor DarkGray
    $scholarUrl = Search-GoogleScholar $full $short $row.Institution
    Start-Sleep -Milliseconds 500
    if ($scholarUrl) {
        Write-Host "$pad      -> $scholarUrl" -ForegroundColor DarkGray
        $sp = Fetch-Page $scholarUrl
        if ($sp -and (Name-IsOnPage $sp $ln)) {
            $hm = [regex]::Match($sp, 'href="(https?://(?!scholar\.google)[^"]+)"[^>]*>Homepage')
            if (-not $hm.Success) { $hm = [regex]::Match($sp, '"url":"(https?://(?!scholar\.google|goo\.gl)[^"]+)"') }
            if ($hm.Success) {
                $sHome = $hm.Groups[1].Value
                Write-Host "$pad      -> homepage: $sHome" -ForegroundColor DarkGray
                if (-not $result.homepage) { $result.homepage = $sHome }
                $hc = Fetch-Page $sHome
                if ($hc -and (Name-IsOnPage $hc $ln)) {
                    $f = Get-BestEmail $hc $domain $fn $ln
                    if ($f -and $f.confidence -notlike "loose*") { $result.email = $f.email; $result.source = "Scholar -> homepage ($($f.confidence))"; return $result }
                } elseif ($hc) { Write-Host "$pad      -> homepage name mismatch" -ForegroundColor DarkYellow }
                Start-Sleep -Milliseconds 200
            }
            $f = Get-BestEmail $sp $domain $fn $ln
            if ($f -and $f.confidence -notlike "loose*") { $result.email = $f.email; $result.source = "Google Scholar ($($f.confidence))"; return $result }
        } elseif ($sp) { Write-Host "$pad      -> wrong profile, skipping" -ForegroundColor DarkYellow }
        Start-Sleep -Milliseconds 300
    } else { Write-Host "$pad      -> no profile found" -ForegroundColor DarkGray }

    # 4. Semantic Scholar
    Write-Host "$pad [4/8] Semantic Scholar..." -ForegroundColor DarkGray
    $ssHome = Search-SemanticScholar $full $short
    Start-Sleep -Milliseconds 300
    if ($ssHome) {
        Write-Host "$pad      -> $ssHome" -ForegroundColor DarkGray
        if (-not $result.homepage) { $result.homepage = $ssHome }
        $sc = Fetch-Page $ssHome
        if ($sc -and (Name-IsOnPage $sc $ln)) {
            $f = Get-BestEmail $sc $domain $fn $ln
            if ($f -and $f.confidence -notlike "loose*") { $result.email = $f.email; $result.source = "Semantic Scholar ($($f.confidence))"; return $result }
        } elseif ($sc) { Write-Host "$pad      -> homepage name mismatch" -ForegroundColor DarkYellow }
    } else { Write-Host "$pad      -> not found" -ForegroundColor DarkGray }

    # 5. ResearchGate
    Write-Host "$pad [5/8] ResearchGate..." -ForegroundColor DarkGray
    foreach ($url in @("https://www.researchgate.net/profile/$fn-$ln", "https://www.researchgate.net/profile/$fn-$ln-1", "https://www.researchgate.net/profile/$($short -replace '\s+','-')") | Select-Object -Unique) {
        $c = Fetch-Page $url
        if ($c -and (Name-IsOnPage $c $ln)) {
            Write-Host "$pad      -> $url" -ForegroundColor DarkGray
            $f = Get-BestEmail $c $domain $fn $ln
            if ($f -and $f.confidence -notlike "loose*") { $result.email = $f.email; $result.source = "ResearchGate ($($f.confidence))"; return $result }
            Write-Host "$pad      -> no email on page" -ForegroundColor DarkGray; break
        }
        Start-Sleep -Milliseconds 300
    }

    # 6. Academia.edu
    Write-Host "$pad [6/8] Academia.edu..." -ForegroundColor DarkGray
    $ac = Fetch-Page "https://www.academia.edu/search?q=$([uri]::EscapeDataString("$full $($row.Institution)"))"
    if (-not $ac -or -not (Name-IsOnPage $ac $ln)) {
        $ac = Fetch-Page "https://www.academia.edu/search?q=$([uri]::EscapeDataString("$short $($row.Institution)"))"
    }
    Start-Sleep -Milliseconds 300
    if ($ac -and (Name-IsOnPage $ac $ln)) {
        $f = Get-BestEmail $ac $domain $fn $ln
        if ($f -and $f.confidence -notlike "loose*") { $result.email = $f.email; $result.source = "Academia.edu ($($f.confidence))"; return $result }
        Write-Host "$pad      -> no email found" -ForegroundColor DarkGray
    } else { Write-Host "$pad      -> no results" -ForegroundColor DarkGray }

    # 7. ORCID
    if ($row.ORCID -and $row.ORCID -ne "") {
        Write-Host "$pad [7/8] ORCID..." -ForegroundColor DarkGray
        $oc = Fetch-Page "https://pub.orcid.org/v3.0/$($row.ORCID -replace 'https://orcid.org/','')/emails"
        if ($oc) {
            $f = Get-BestEmail $oc $domain $fn $ln
            if ($f -and $f.confidence -notlike "loose*") { $result.email = $f.email; $result.source = "ORCID ($($f.confidence))"; return $result }
            Write-Host "$pad      -> no email visible" -ForegroundColor DarkGray
        } else { Write-Host "$pad      -> no response" -ForegroundColor DarkGray }
        Start-Sleep -Milliseconds 200
    } else { Write-Host "$pad [7/8] ORCID... no ORCID on record" -ForegroundColor DarkGray }

    # 8. University directory
    Write-Host "$pad [8/8] University directory ($domain)..." -ForegroundColor DarkGray
    if ($domain) {
        $encFull  = [uri]::EscapeDataString($full)
        $encShort = [uri]::EscapeDataString($short)
        foreach ($url in @("https://$domain/directory?search=$encFull","https://$domain/directory?search=$encShort","https://$domain/people?search=$encShort","https://$domain/faculty?search=$encShort","https://directory.$domain/?q=$encShort","https://www.$domain/search?q=$encShort") | Select-Object -Unique) {
            Write-Host "$pad      -> $url" -ForegroundColor DarkGray
            $c = Fetch-Page $url
            if ($c -and (Name-IsOnPage $c $ln)) {
                $f = Get-BestEmail $c $domain $fn $ln
                if ($f -and $f.confidence -notlike "loose*") { $result.email = $f.email; $result.source = "university directory ($($f.confidence))"; return $result }
            }
            Start-Sleep -Milliseconds 150
        }
    }

    return $result
}

# =============================================================================
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Scholars to Storytellers - Full Pipeline" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$pipelineStart   = Get-Date
$sfDoneFile      = "$WorkDir\SOP_SF_Done.csv"
$seenAuthorsFile = "$WorkDir\SOP_Seen_IDs.csv"

#  Load which subfields are already complete 
$doneSubfields = @{}
if (Test-Path $sfDoneFile) {
    foreach ($r in (Import-Csv $sfDoneFile)) { $doneSubfields[$r.Subfield] = $true }
    Write-Host "Resuming - completed subfields: $($doneSubfields.Keys -join ', ')" -ForegroundColor Yellow
    Write-Host ""
}

#  Load globally seen author IDs (avoid duplicates across subfields) 
$seenIds = @{}
if (Test-Path $seenAuthorsFile) {
    foreach ($r in (Import-Csv $seenAuthorsFile)) { $seenIds[$r.ID] = $true }
    Write-Host "Loaded $($seenIds.Count) previously seen author IDs." -ForegroundColor DarkGray
    Write-Host ""
}

#  Counters across all subfields 
$globalAuthors = 0
$globalMatched = 0
$globalNoEmail = 0

# =============================================================================
# MAIN LOOP  one subfield (department) at a time, end-to-end
# =============================================================================
foreach ($sf in $targetSubfields) {

    if ($doneSubfields.ContainsKey($sf.Name)) {
        Write-Host "[$($sf.Name)] already complete, skipping." -ForegroundColor DarkGray
        continue
    }

    Write-Host ""
    Write-Host "============================================" -ForegroundColor Cyan
    Write-Host "  DEPARTMENT: $($sf.Name)" -ForegroundColor Cyan
    Write-Host "============================================" -ForegroundColor Cyan
    $sfStart = Get-Date

    # -------------------------------------------------------------------------
    # PHASE 1 -- Scan highly-cited works (cited_by_count>200) to collect author IDs
    # Filters out low-impact papers, keeps ~40-60 pages instead of 700+
    # -------------------------------------------------------------------------
    $sfSafe       = $sf.Name -replace '[^a-zA-Z0-9]','_'
    $p1CkFile     = "$WorkDir\SOP_CK_P1_${sfSafe}.csv"
    $p1CursorFile = "$WorkDir\SOP_CK_P1_${sfSafe}_cursor.txt"

    $sfAuthorFieldMap = @{}
    $cursor = "*"; $worksScanned = 0; $pageCount = 0

    if ((Test-Path $p1CkFile) -and (Test-Path $p1CursorFile)) {
        foreach ($r in (Import-Csv $p1CkFile)) { $sfAuthorFieldMap[$r.AuthorId] = $r.Field }
        $cursor = (Get-Content $p1CursorFile -Raw).Trim()
        Write-Host "  [Phase 1] Resuming from checkpoint: $($sfAuthorFieldMap.Count) authors" -ForegroundColor Yellow
    } else {
        Write-Host "  [Phase 1] Scanning highly-cited works (citations > 100)..." -ForegroundColor Cyan
    }

    while ($true) {
        $url = "https://api.openalex.org/works?" +
               "filter=institutions.country_code:US,primary_topic.subfield.id:$($sf.Id),cited_by_count:>200" +
               $amp + "sort=cited_by_count:desc" +
               $amp + "per_page=200" +
               $amp + "cursor=" + [uri]::EscapeDataString($cursor) +
               $amp + "select=id,authorships"
        $data = Invoke-OA $url
        Start-Sleep -Milliseconds $DelayMs
        if (-not $data -or -not $data.results -or $data.results.Count -eq 0) { break }

        foreach ($work in $data.results) {
            foreach ($auth in $work.authorships) {
                if (-not $auth.author -or -not $auth.author.id) { continue }
                $hasUS = $false
                foreach ($inst in $auth.institutions) {
                    if ($inst.country_code -eq "US" -and $inst.type -in @("education","nonprofit")) { $hasUS = $true; break }
                }
                if (-not $hasUS) { continue }
                $aId = $auth.author.id -replace "https://openalex.org/",""
                if (-not $sfAuthorFieldMap.ContainsKey($aId) -and -not $seenIds.ContainsKey($aId)) {
                    $sfAuthorFieldMap[$aId] = $sf.Name
                }
            }
        }
        $worksScanned += $data.results.Count
        $pageCount++
        Write-Host "    -> page $pageCount | $worksScanned works | $($sfAuthorFieldMap.Count) authors" -ForegroundColor DarkGray

        if ($pageCount % 5 -eq 0) {
            $sfAuthorFieldMap.Keys | ForEach-Object { [PSCustomObject]@{ AuthorId=$_; Field=$sfAuthorFieldMap[$_] } } |
                Export-Csv -Path $p1CkFile -NoTypeInformation -Encoding UTF8
            Set-Content -Path $p1CursorFile -Value $cursor -Encoding UTF8
            Write-Host "    [CHECKPOINT saved at page $pageCount]" -ForegroundColor DarkGreen
        }

        if ($data.meta -and $data.meta.next_cursor -and $data.meta.next_cursor -ne $cursor) { $cursor = $data.meta.next_cursor } else { break }
        if ($data.results.Count -lt 200) { break }
    }

    if (Test-Path $p1CkFile)    { Remove-Item $p1CkFile    -Force }
    if (Test-Path $p1CursorFile){ Remove-Item $p1CursorFile -Force }
    Write-Host "  [Phase 1] DONE: $worksScanned works -> $($sfAuthorFieldMap.Count) author IDs" -ForegroundColor Green

    if ($sfAuthorFieldMap.Count -eq 0) {
        if ($worksScanned -eq 0) {
            Write-Host "  [WARNING] 0 works scanned for $($sf.Name) - likely API error, will retry next run." -ForegroundColor Red
        } else {
            Write-Host "  No US authors found in $($sf.Name) works, marking done." -ForegroundColor Yellow
            [PSCustomObject]@{ Subfield=$sf.Name; CompletedAt=(Get-Date).ToString("yyyy-MM-dd HH:mm") } |
                Export-Csv -Path $sfDoneFile -Append -NoTypeInformation -Encoding UTF8
            $doneSubfields[$sf.Name] = $true
        }
        continue
    }

    # -------------------------------------------------------------------------
    # PHASE 2a -- Enrich authors (filter by thresholds, US institution, not hard sci)
    # -------------------------------------------------------------------------
    Write-Host "  [Phase 2a] Enriching $($sfAuthorFieldMap.Count) authors..." -ForegroundColor Cyan

    $sfAuthorData = @{}
    $chunks = Split-Chunks @($sfAuthorFieldMap.Keys) $AuthorBatch
    $cc = 0

    foreach ($chunk in $chunks) {
        $cc++
        $url  = "https://api.openalex.org/authors?filter=openalex_id:" + ($chunk -join "|") +
                $amp + "select=id,display_name,ids,last_known_institutions,works_count,cited_by_count,x_concepts" +
                $amp + "per_page=$AuthorBatch"
        $resp = Invoke-OA $url
        Start-Sleep -Milliseconds $DelayMs

        if ($resp -and $resp.results) {
            foreach ($a in $resp.results) {
                $aShort = $a.id -replace "https://openalex.org/",""
                if ($a.works_count -lt $MinWorksCount -or $a.cited_by_count -lt $MinCitedBy) { continue }
                $usInst = $null
                if ($a.last_known_institutions) { foreach ($inst in $a.last_known_institutions) { if ($inst.country_code -eq "US") { $usInst = $inst; break } } }
                if (-not $usInst) { continue }
                $instLower = $usInst.display_name.ToLower()
                $isBadInst = $false
                foreach ($bk in $badInstKeywords) { if ($instLower -like "*$bk*") { $isBadInst = $true; break } }
                if ($isBadInst) { continue }
                $topics = if ($a.x_concepts) { ($a.x_concepts | Select-Object -First 4 | ForEach-Object { $_.display_name }) -join "; " } else { "" }
                $isHard = $false
                foreach ($topicPart in ($topics -split ";")) {
                    $t = $topicPart.Trim().ToLower()
                    foreach ($hs in $hardSciList) { if ($t -like "*$hs*") { $isHard = $true; break } }
                    if ($isHard) { break }
                }
                if ($isHard) { continue }
                $sfAuthorData[$aShort] = @{
                    Id=$aShort; DisplayName=$a.display_name
                    ORCID=if ($a.ids -and $a.ids.orcid) { $a.ids.orcid } else { "" }
                    Institution=$usInst.display_name; Field=$sf.Name; Topics=$topics
                    WorksCount=$a.works_count; CitedBy=$a.cited_by_count
                    HasAcademicBook=$false; HasTradeBook=$false
                    AcademicBooks=[System.Collections.ArrayList]::new()
                }
            }
        }
        if ($cc % 20 -eq 0) { Write-Host "    [$cc/$($chunks.Count)] Qualified: $($sfAuthorData.Count)" }
    }
    Write-Host "  [Phase 2a] $($sfAuthorData.Count) authors passed thresholds" -ForegroundColor Green

    if ($sfAuthorData.Count -eq 0) {
        Write-Host "  No qualified authors for $($sf.Name), marking done." -ForegroundColor Yellow
        [PSCustomObject]@{ Subfield=$sf.Name; CompletedAt=(Get-Date).ToString("yyyy-MM-dd HH:mm") } |
            Export-Csv -Path $sfDoneFile -Append -NoTypeInformation -Encoding UTF8
        $doneSubfields[$sf.Name] = $true
        continue
    }

    # Pause before book classification to let rate limit recover
    Write-Host "  Pausing 20s before book classification..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 20

    # -------------------------------------------------------------------------
    # PHASE 2b  Book classification (academic vs trade)
    # -------------------------------------------------------------------------
    Write-Host "  [Phase 2b] Classifying books..." -ForegroundColor Cyan

    $qualIds    = @($sfAuthorData.Keys)
    $bookChunks = Split-Chunks $qualIds $BookBatch
    $bc = 0

    foreach ($chunk in $bookChunks) {
        $bc++
        $url  = "https://api.openalex.org/works?filter=author.id:" + ($chunk -join "|") + ",type:book|book-chapter" +
                $amp + "select=id,title,publication_year,primary_location,authorships" +
                $amp + "per_page=100"
        $resp = Invoke-OA $url
        Start-Sleep -Milliseconds 1200   # stay well under rate limit in Phase 2b
        if (-not $resp -or -not $resp.results) { continue }

        foreach ($book in $resp.results) {
            $pubName = ""
            if ($book.primary_location -and $book.primary_location.source) {
                $src     = $book.primary_location.source
                $pubName = if ($src.host_organization_name) { $src.host_organization_name }
                           elseif ($src.display_name)        { $src.display_name }
                           else                              { "" }
            }
            $pubLower = $pubName.ToLower()
            $isTrade  = $false
            foreach ($tp in $tradePublishers) { if ($pubLower -like "*$tp*") { $isTrade = $true; break } }

            foreach ($auth in $book.authorships) {
                if (-not $auth.author -or -not $auth.author.id) { continue }
                $aShort = $auth.author.id -replace "https://openalex.org/",""
                if (-not $sfAuthorData.ContainsKey($aShort)) { continue }
                if ($isTrade) {
                    $sfAuthorData[$aShort].HasTradeBook = $true
                } else {
                    $sfAuthorData[$aShort].HasAcademicBook = $true
                    $label = if ($book.title -and $book.publication_year) { "$($book.title) ($($book.publication_year))" } elseif ($book.title) { $book.title } else { "" }
                    if ($label -and $sfAuthorData[$aShort].AcademicBooks -notcontains $label) { [void]$sfAuthorData[$aShort].AcademicBooks.Add($label) }
                }
            }
        }
    }

    $acad  = ($sfAuthorData.Values | Where-Object { -not $_.HasTradeBook }).Count
    $trade = ($sfAuthorData.Values | Where-Object { $_.HasTradeBook }).Count
    Write-Host "  [Phase 2b] No trade book: $acad | Trade (excluded): $trade" -ForegroundColor Green

    # -------------------------------------------------------------------------
    # PHASE 3a  Resolve institution domains
    # -------------------------------------------------------------------------
    Write-Host "  [Phase 3a] Resolving institution domains..." -ForegroundColor Cyan

    $uniqueInsts = @($sfAuthorData.Values |
        Where-Object { -not $_.HasTradeBook } |
        ForEach-Object { $_.Institution } | Select-Object -Unique | Where-Object { $_ })

    $domainCache = @{}
    $dc = 0; $df = 0

    foreach ($instName in $uniqueInsts) {
        $dc++
        try {
            $rorResp = Invoke-RestMethod -Uri "https://api.ror.org/organizations?affiliation=$([uri]::EscapeDataString($instName))" -Method Get -TimeoutSec 15 -ErrorAction Stop
            if ($rorResp.items -and $rorResp.items.Count -gt 0) {
                $org  = ($rorResp.items | Sort-Object score -Descending | Select-Object -First 1).organization
                if ($org -and $org.links -and $org.links.Count -gt 0) {
                    $link    = $org.links | Where-Object { $_.type -eq "website" } | Select-Object -First 1
                    if (-not $link) { $link = $org.links[0] }
                    $siteUrl = if ($link -is [string]) { $link } elseif ($link.value) { $link.value } else { "" }
                    $domain  = ($siteUrl -replace "https?://(www\.)?","" -replace "/.*","").Trim().ToLower()
                    if ($domain) { $df++; $domainCache[$instName] = $domain }
                }
            }
        } catch {}
        if (-not $domainCache.ContainsKey($instName)) { $domainCache[$instName] = "" }
        Start-Sleep -Milliseconds 200
    }
    Write-Host "  [Phase 3a] $df/$($uniqueInsts.Count) domains resolved" -ForegroundColor Green

    # -------------------------------------------------------------------------
    # PHASE 3b  Build author rows for this subfield
    # -------------------------------------------------------------------------
    Write-Host "  [Phase 3b] Building author rows..." -ForegroundColor Cyan

    $sfRows = [System.Collections.ArrayList]::new()
    foreach ($aId in $sfAuthorData.Keys) {
        $d = $sfAuthorData[$aId]
        if ($d.HasTradeBook) { continue }
        $domain   = if ($domainCache.ContainsKey($d.Institution)) { $domainCache[$d.Institution] } else { "" }
        $bookList = if ($d.AcademicBooks.Count -gt 0) { $d.AcademicBooks -join " | " } else { "" }
        $bookStatus = if ($d.HasAcademicBook) { "Academic books" } else { "No books yet" }
        [void]$sfRows.Add([PSCustomObject]@{
            Name               = $d.DisplayName
            Department         = $d.Field
            Institution        = $d.Institution
            Institution_Domain = $domain
            ORCID              = $d.ORCID
            Research_Topics    = $d.Topics
            Book_Status        = $bookStatus
            Academic_Books     = $bookList
            Works_Count        = $d.WorksCount
            Cited_By_Count     = $d.CitedBy
            OpenAlex_ID        = "https://openalex.org/" + $aId
            OpenAlex_Profile   = "https://openalex.org/" + $aId
        })
        # Mark as seen globally
        $seenIds[$aId] = $true
    }
    $sfRows = @($sfRows | Sort-Object Cited_By_Count -Descending)

    # Append to master authors CSV
    $sfRows | Export-Csv -Path $authorsCsv -Append -NoTypeInformation -Encoding UTF8

    # Save updated seen IDs
    $seenIds.Keys | ForEach-Object { [PSCustomObject]@{ ID = $_ } } |
        Export-Csv -Path $seenAuthorsFile -NoTypeInformation -Encoding UTF8

    Write-Host "  [Phase 3b] $($sfRows.Count) authors qualify (no trade book)" -ForegroundColor Green
    $globalAuthors += $sfRows.Count

    if ($sfRows.Count -eq 0) {
        Write-Host "  No qualifying authors for $($sf.Name), marking done." -ForegroundColor Yellow
        [PSCustomObject]@{ Subfield = $sf.Name; CompletedAt = (Get-Date).ToString("yyyy-MM-dd HH:mm") } |
            Export-Csv -Path $sfDoneFile -Append -NoTypeInformation -Encoding UTF8
        $doneSubfields[$sf.Name] = $true
        continue
    }

    # -------------------------------------------------------------------------
    # PHASE 4  Email enrichment for this subfield
    # -------------------------------------------------------------------------
    Write-Host "  [Phase 4] Email enrichment for $($sfRows.Count) authors..." -ForegroundColor Cyan
    Write-Host ""

    $counter = 0; $matched = 0; $noMatch = 0

    foreach ($row in $sfRows) {
        $counter++
        Write-Host "[$counter/$($sfRows.Count)] $($row.Name) @ $($row.Institution)" -ForegroundColor White
        Write-Progress -Activity "[$($sf.Name)] Email enrichment" -Status "$counter/$($sfRows.Count) | found: $matched" -PercentComplete ([math]::Round($counter/$sfRows.Count*100))

        $res = Find-Email $row

        if ($res.email) {
            $matched++
            Write-Host "  => FOUND: $($res.email) [$($res.source)]" -ForegroundColor Green
        } else {
            $noMatch++
            Write-Host "  => no email found" -ForegroundColor Yellow
        }

        $outRow = [PSCustomObject]@{
            Name               = $row.Name
            Department         = $row.Department
            Institution        = $row.Institution
            Faculty_URL        = $res.homepage
            Email              = $res.email
            Bio                = ""
            Book_Status        = $row.Book_Status
            Books              = $row.Academic_Books
            Research_Topics    = $row.Research_Topics
            Works_Count        = $row.Works_Count
            Cited_By_Count     = $row.Cited_By_Count
            Email_Source       = $res.source
            Institution_Domain = $row.Institution_Domain
            ORCID              = $row.ORCID
            OpenAlex_ID        = $row.OpenAlex_ID
            OpenAlex_Profile   = $row.OpenAlex_Profile
        }

        # Append immediately to the right file
        if ($res.email) {
            $outRow | Export-Csv -Path $withEmailCsv -Append -NoTypeInformation -Encoding UTF8
        } else {
            $outRow | Export-Csv -Path $noEmailCsv -Append -NoTypeInformation -Encoding UTF8
        }

        # Save checkpoint every 5 authors  minimise power-cut losses
        if ($counter % 5 -eq 0) {
            $pct     = [math]::Round($matched / [math]::Max($counter,1) * 100)
            $elapsed = [math]::Round(((Get-Date) - $sfStart).TotalMinutes, 1)
            $rate    = ((Get-Date) - $sfStart).TotalSeconds / $counter
            $etaMins = [math]::Round($rate * ($sfRows.Count - $counter) / 60, 0)
            Write-Host ""
            Write-Host "  --- [$($sf.Name)] [$elapsed min] $counter/$($sfRows.Count) | $matched emails ($pct%) | ETA ~${etaMins}m ---" -ForegroundColor Cyan
            Write-Host "  --- SOP_With_Emails.csv updated ---" -ForegroundColor Green
            Write-Host ""
        }

        Start-Sleep -Milliseconds $EmailDelayMs
    }

    Write-Progress -Activity "[$($sf.Name)] Email enrichment" -Completed

    $globalMatched += $matched
    $globalNoEmail += $noMatch
    $sfMins = [math]::Round(((Get-Date) - $sfStart).TotalMinutes, 1)
    $sfPct  = [math]::Round($matched / [math]::Max($counter,1) * 100)

    Write-Host ""
    Write-Host "  [$($sf.Name)] DONE in $sfMins min | $counter authors | $matched emails ($sfPct%)" -ForegroundColor Green

    # Mark subfield complete
    [PSCustomObject]@{ Subfield = $sf.Name; CompletedAt = (Get-Date).ToString("yyyy-MM-dd HH:mm") } |
        Export-Csv -Path $sfDoneFile -Append -NoTypeInformation -Encoding UTF8
    $doneSubfields[$sf.Name] = $true

} # end foreach subfield

# =============================================================================
# SUMMARY
# =============================================================================
$totalMins = [math]::Round(((Get-Date) - $pipelineStart).TotalMinutes, 1)
$matchPct  = [math]::Round($globalMatched / [math]::Max($globalAuthors,1) * 100)

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Pipeline Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Total runtime   : $totalMins min"
Write-Host "Authors pulled  : $globalAuthors"
Write-Host "Emails found    : $globalMatched ($matchPct%)" -ForegroundColor Green
Write-Host "No email        : $globalNoEmail"
Write-Host ""
Write-Host "Output files:"
Write-Host "  $withEmailCsv" -ForegroundColor Green
Write-Host "  $noEmailCsv" -ForegroundColor Yellow
Write-Host ""

if (Test-Path $withEmailCsv) {
    $sample = @(Import-Csv $withEmailCsv | Select-Object -First 10)
    if ($sample.Count -gt 0) {
        Write-Host "Sample with emails:"
        $sample | Format-Table Name, Institution, Email, Email_Source -AutoSize
    }
}

# =============================================================================
# PHASE 1  Collect author IDs from OpenAlex works
# =============================================================================
Write-Host "PHASE 1: Scanning OpenAlex works in 9 SOP subfields..." -ForegroundColor Cyan
Write-Host ""

$p1CheckpointCsv = "$WorkDir\SOP_Phase1_Checkpoint.csv"
$authorFieldMap  = @{}
$doneSubfields   = @{}
$p1Start         = Get-Date

# Resume Phase 1 if checkpoint exists
if (Test-Path $p1CheckpointCsv) {
    Write-Host "  Phase 1 checkpoint found - loading..." -ForegroundColor Yellow
    foreach ($r in (Import-Csv $p1CheckpointCsv)) {
        $authorFieldMap[$r.AuthorId] = $r.Field
        $doneSubfields[$r.Field] = $true
    }
    Write-Host "  Loaded $($authorFieldMap.Count) authors, skipping completed subfields: $($doneSubfields.Keys -join ', ')" -ForegroundColor Yellow
    Write-Host ""
}

foreach ($sf in $targetSubfields) {
    if ($doneSubfields.ContainsKey($sf.Name)) {
        Write-Host "  [$($sf.Name)] already done, skipping." -ForegroundColor DarkGray
        continue
    }

    $cursor = "*"; $worksScanned = 0; $authorsAdded = 0
    Write-Host "  [$($sf.Name)] scanning..." -NoNewline

    while ($true) {
        $fPart   = "filter=institutions.country_code:US,primary_topic.subfield.id:" + $sf.Id
        $url     = "https://api.openalex.org/works?" + $fPart + $amp + "sort=cited_by_count:desc" + $amp + "per_page=200" + $amp + "cursor=" + [uri]::EscapeDataString($cursor) + $amp + "select=id,authorships"
        $data    = Invoke-OA $url
        Start-Sleep -Milliseconds $DelayMs
        if (-not $data -or -not $data.results -or $data.results.Count -eq 0) { break }

        foreach ($work in $data.results) {
            foreach ($auth in $work.authorships) {
                if (-not $auth.author -or -not $auth.author.id) { continue }
                $hasUS = $false
                foreach ($inst in $auth.institutions) {
                    if ($inst.country_code -eq "US" -and $inst.type -in @("education","nonprofit")) { $hasUS = $true; break }
                }
                if (-not $hasUS) { continue }
                $aId = $auth.author.id -replace "https://openalex.org/",""
                if (-not $authorFieldMap.ContainsKey($aId)) { $authorFieldMap[$aId] = $sf.Name; $authorsAdded++ }
            }
        }
        $worksScanned += $data.results.Count
        if ($data.meta -and $data.meta.next_cursor -and $data.meta.next_cursor -ne $cursor) { $cursor = $data.meta.next_cursor } else { break }
        if ($data.results.Count -lt 200) { break }
    }

    Write-Host " $worksScanned works -> $authorsAdded new authors (total: $($authorFieldMap.Count))" -ForegroundColor Green

    # Save checkpoint after each subfield completes
    $authorFieldMap.GetEnumerator() | ForEach-Object {
        [PSCustomObject]@{ AuthorId = $_.Key; Field = $_.Value }
    } | Export-Csv -Path $p1CheckpointCsv -NoTypeInformation -Encoding UTF8
    Write-Host "  Checkpoint saved." -ForegroundColor DarkGray
}

Write-Host ""
Write-Host "Phase 1 done in $([math]::Round(((Get-Date)-$p1Start).TotalMinutes,1)) min: $($authorFieldMap.Count) unique US authors" -ForegroundColor Green

# Clean up Phase 1 checkpoint once fully done
if (Test-Path $p1CheckpointCsv) { Remove-Item $p1CheckpointCsv -Force }
Write-Host ""

# =============================================================================
# PHASE 2a  Batch author enrichment
# =============================================================================
Write-Host "PHASE 2a: Batch author enrichment..." -ForegroundColor Cyan

$allAuthorIds = @($authorFieldMap.Keys)
$authorData   = @{}
$chunks       = Split-Chunks $allAuthorIds $AuthorBatch
$cc = 0

foreach ($chunk in $chunks) {
    $cc++
    $idList  = $chunk -join "|"
    $url     = "https://api.openalex.org/authors?filter=openalex_id:" + $idList + $amp + "select=id,display_name,ids,last_known_institutions,works_count,cited_by_count,x_concepts" + $amp + "per_page=$AuthorBatch"
    $resp    = Invoke-OA $url
    Start-Sleep -Milliseconds $DelayMs

    if ($resp -and $resp.results) {
        foreach ($a in $resp.results) {
            $aShort = $a.id -replace "https://openalex.org/",""
            if ($a.works_count -lt $MinWorksCount -or $a.cited_by_count -lt $MinCitedBy) { continue }
            $usInst = $null
            if ($a.last_known_institutions) { foreach ($inst in $a.last_known_institutions) { if ($inst.country_code -eq "US") { $usInst = $inst; break } } }
            if (-not $usInst) { continue }
            $topics = if ($a.x_concepts) { ($a.x_concepts | Select-Object -First 4 | ForEach-Object { $_.display_name }) -join "; " } else { "" }
            $firstTopic = ($topics -split ";")[0].Trim().ToLower()
            $isHard = $false
            foreach ($hs in $hardSciList) { if ($firstTopic -like "*$hs*") { $isHard = $true; break } }
            if ($isHard) { continue }
            $authorData[$aShort] = @{
                Id              = $aShort; DisplayName = $a.display_name
                ORCID           = if ($a.ids -and $a.ids.orcid) { $a.ids.orcid } else { "" }
                Institution     = $usInst.display_name
                Field           = if ($authorFieldMap.ContainsKey($aShort)) { $authorFieldMap[$aShort] } else { "" }
                Topics          = $topics; WorksCount = $a.works_count; CitedBy = $a.cited_by_count
                HasAcademicBook = $false
                HasTradeBook    = $false
                AcademicBooks   = [System.Collections.ArrayList]::new()
            }
        }
    }
    if ($cc % 100 -eq 0) { Write-Host "  [$cc/$($chunks.Count)] Qualified: $($authorData.Count)" }
}
Write-Host "Phase 2a done: $($authorData.Count) authors passed thresholds" -ForegroundColor Green
Write-Host ""

# =============================================================================
# PHASE 2b  Book classification (academic vs trade)
# =============================================================================
Write-Host "PHASE 2b: Classifying books and book chapters (academic vs trade)..." -ForegroundColor Cyan

$qualIds    = @($authorData.Keys)
$bookChunks = Split-Chunks $qualIds $BookBatch
$bc = 0

foreach ($chunk in $bookChunks) {
    $bc++
    $url  = "https://api.openalex.org/works?filter=author.id:" + ($chunk -join "|") + ",type:book|book-chapter" + $amp + "select=id,title,publication_year,primary_location,authorships" + $amp + "per_page=100"
    $resp = Invoke-OA $url
    Start-Sleep -Milliseconds $DelayMs
    if (-not $resp -or -not $resp.results) { continue }

    foreach ($book in $resp.results) {
        $pubName = ""
        if ($book.primary_location -and $book.primary_location.source) {
            $src = $book.primary_location.source
            $pubName = if ($src.host_organization_name) { $src.host_organization_name }
                       elseif ($src.display_name)        { $src.display_name }
                       else                              { "" }
        }
        $pubLower = $pubName.ToLower()
        $isTrade  = $false
        foreach ($tp in $script:tradePublishers) { if ($pubLower -like "*$tp*") { $isTrade = $true; break } }

        foreach ($auth in $book.authorships) {
            if (-not $auth.author -or -not $auth.author.id) { continue }
            $aShort = $auth.author.id -replace "https://openalex.org/",""
            if (-not $authorData.ContainsKey($aShort)) { continue }
            if ($isTrade) {
                $authorData[$aShort].HasTradeBook = $true
            } else {
                $authorData[$aShort].HasAcademicBook = $true
                $label = if ($book.title -and $book.publication_year) { "$($book.title) ($($book.publication_year))" } elseif ($book.title) { $book.title } else { "" }
                if ($label -and $authorData[$aShort].AcademicBooks -notcontains $label) { [void]$authorData[$aShort].AcademicBooks.Add($label) }
            }
        }
    }
    if ($bc % 100 -eq 0) {
        $noTrade = ($authorData.Values | Where-Object { -not $_.HasTradeBook }).Count
        $trade   = ($authorData.Values | Where-Object { $_.HasTradeBook }).Count
        Write-Host "  [$bc/$($bookChunks.Count)] No-trade: $noTrade | Trade (excluded): $trade"
    }
}

$withAcademic = ($authorData.Values | Where-Object { $_.HasAcademicBook -and -not $_.HasTradeBook }).Count
$withTrade    = ($authorData.Values | Where-Object { $_.HasTradeBook }).Count
$noBooks      = ($authorData.Values | Where-Object { -not $_.HasAcademicBook -and -not $_.HasTradeBook }).Count
Write-Host "Phase 2b done:" -ForegroundColor Green
Write-Host "  Academic books only : $withAcademic (QUALIFY)" -ForegroundColor Green
Write-Host "  No books yet        : $noBooks (QUALIFY - prime targets)" -ForegroundColor Green
Write-Host "  Has trade book      : $withTrade (excluded)"
Write-Host ""

# =============================================================================
# PHASE 3a  Institution domains via ROR
# =============================================================================
Write-Host "PHASE 3a: Resolving institution domains via ROR..." -ForegroundColor Cyan

$uniqueInsts = @($authorData.Values | Where-Object { -not $_.HasTradeBook } | ForEach-Object { $_.Institution } | Select-Object -Unique | Where-Object { $_ })
$domainCache = @{}
$dc = 0; $df = 0

foreach ($instName in $uniqueInsts) {
    $dc++
    Write-Progress -Activity "Resolving domains" -Status "[$dc/$($uniqueInsts.Count)] $instName" -PercentComplete ([math]::Round($dc/$uniqueInsts.Count*100))
    $domain = ""
    try {
        $rorResp = Invoke-RestMethod -Uri "https://api.ror.org/organizations?affiliation=$([uri]::EscapeDataString($instName))" -Method Get -TimeoutSec 15 -ErrorAction Stop
        if ($rorResp.items -and $rorResp.items.Count -gt 0) {
            $org = ($rorResp.items | Sort-Object score -Descending | Select-Object -First 1).organization
            if ($org -and $org.links -and $org.links.Count -gt 0) {
                $link    = $org.links | Where-Object { $_.type -eq "website" } | Select-Object -First 1
                if (-not $link) { $link = $org.links[0] }
                $siteUrl = if ($link -is [string]) { $link } elseif ($link.value) { $link.value } else { "" }
                $domain  = ($siteUrl -replace "https?://(www\.)?","" -replace "/.*","").Trim().ToLower()
                if ($domain) { $df++ }
            }
        }
    } catch {}
    $domainCache[$instName] = $domain
    Start-Sleep -Milliseconds 200
    if ($dc % 100 -eq 0) { Write-Host "  [$dc/$($uniqueInsts.Count)] domains found: $df" -ForegroundColor Green }
}
Write-Progress -Activity "Resolving domains" -Completed
Write-Host "Phase 3a done: $df / $($uniqueInsts.Count) domains resolved" -ForegroundColor Green
Write-Host ""

# =============================================================================
# PHASE 3b  Build and save authors CSV
# =============================================================================
Write-Host "PHASE 3b: Building authors list..." -ForegroundColor Cyan

$results = New-Object System.Collections.ArrayList
foreach ($aId in $authorData.Keys) {
    $d = $authorData[$aId]
    if ($d.HasTradeBook) { continue }
    $domain     = if ($domainCache.ContainsKey($d.Institution)) { $domainCache[$d.Institution] } else { "" }
    $bookList   = if ($d.AcademicBooks -and $d.AcademicBooks.Count -gt 0) { $d.AcademicBooks -join " | " } else { "" }
    $bookStatus = if ($d.HasAcademicBook) { "Academic books" } else { "No books yet" }
    [void]$results.Add([PSCustomObject]@{
        Author             = $d.DisplayName
        OpenAlex_ID        = "https://openalex.org/" + $aId
        ORCID              = $d.ORCID
        Institution        = $d.Institution
        Institution_Domain = $domain
        Primary_Field      = $d.Field
        Research_Topics    = $d.Topics
        Book_Status        = $bookStatus
        Academic_Books     = $bookList
        Works_Count        = $d.WorksCount
        Cited_By_Count     = $d.CitedBy
        OpenAlex_Profile   = "https://openalex.org/" + $aId
    })
}
$allAuthors = @($results | Sort-Object Cited_By_Count -Descending)
$allAuthors | Export-Csv -Path $authorsCsv -NoTypeInformation -Encoding UTF8
Write-Host "Phase 3b done: $($allAuthors.Count) authors saved to $authorsCsv" -ForegroundColor Green
Write-Host ""

# =============================================================================
# PHASE 4  Web email enrichment
# =============================================================================
if ($allAuthors.Count -eq 0) {
    Write-Host "ERROR: No authors were loaded or pulled. Check OpenAlex connectivity and try again." -ForegroundColor Red
    Write-Host "  If SOP_Authors_Full.csv exists and is empty, delete it and re-run." -ForegroundColor Yellow
    exit 1
}
Write-Host "PHASE 4: Web email enrichment ($($allAuthors.Count) authors)..." -ForegroundColor Cyan
Write-Host ""

$doneIds = @{}
$enriched = New-Object System.Collections.ArrayList

if (Test-Path $checkpointCsv) {
    $ckpt = Import-Csv $checkpointCsv
    foreach ($r in $ckpt) { $doneIds[$r.OpenAlex_ID] = $true; [void]$enriched.Add($r) }
    Write-Host "Checkpoint found - $($doneIds.Count) already done, resuming..." -ForegroundColor Yellow
    Write-Host ""
}

$counter = 0; $matched = 0; $noMatch = 0
$toProcess = @($allAuthors | Where-Object { $_.OpenAlex_ID -and -not $doneIds.ContainsKey($_.OpenAlex_ID) })

foreach ($row in $toProcess) {
    $counter++
    Write-Host "[$counter/$($toProcess.Count)] $($row.Author) @ $($row.Institution)" -ForegroundColor White
    Write-Progress -Activity "Email enrichment" -Status "[$counter/$($toProcess.Count)] found: $matched" -PercentComplete ([math]::Round($counter/$toProcess.Count*100))

    $res = Find-Email $row

    if ($res.email) { $matched++; Write-Host "  => FOUND: $($res.email) [$($res.source)]" -ForegroundColor Green }
    else            { $noMatch++;  Write-Host "  => no email found" -ForegroundColor Yellow }

    [void]$enriched.Add([PSCustomObject]@{
        Author             = $row.Author
        OpenAlex_ID        = $row.OpenAlex_ID
        ORCID              = $row.ORCID
        Institution        = $row.Institution
        Institution_Domain = $row.Institution_Domain
        Primary_Field      = $row.Primary_Field
        Research_Topics    = $row.Research_Topics
        Book_Status        = $row.Book_Status
        Academic_Books     = $row.Academic_Books
        Works_Count        = $row.Works_Count
        Cited_By_Count     = $row.Cited_By_Count
        Email              = $res.email
        Email_Source       = $res.source
        Homepage           = $res.homepage
        OpenAlex_Profile   = $row.OpenAlex_Profile
    })

    # Save all output files every 25 authors  open anytime to see latest results
    if ($counter % 25 -eq 0) {
        $enriched | Export-Csv -Path $checkpointCsv -NoTypeInformation -Encoding UTF8

        $snap = @($enriched | Where-Object { $_.Email -ne "" })
        $snap | Export-Csv -Path $withEmailCsv -NoTypeInformation -Encoding UTF8
        @($enriched | Where-Object { $_.Email -eq "" }) | Export-Csv -Path $noEmailCsv -NoTypeInformation -Encoding UTF8

        $pct      = [math]::Round($matched / [math]::Max($counter,1) * 100)
        $elapsed  = [math]::Round(((Get-Date) - $pipelineStart).TotalMinutes, 1)
        $rate     = if ($counter -gt 0) { ((Get-Date) - $pipelineStart).TotalSeconds / $counter } else { 5 }
        $etaMins  = [math]::Round($rate * ($toProcess.Count - $counter) / 60, 0)
        Write-Host ""
        Write-Host "  --- [$elapsed min] $counter/$($toProcess.Count) processed | $matched emails ($pct%) | ETA ~${etaMins}m ---" -ForegroundColor Cyan
        Write-Host "  --- SOP_With_Emails.csv updated ($($snap.Count) rows) ---" -ForegroundColor Green
        Write-Host ""
    }

    Start-Sleep -Milliseconds $EmailDelayMs
}

Write-Progress -Activity "Email enrichment" -Completed

# Final save
$withEmail = @($enriched | Where-Object { $_.Email -ne "" })
$noEmail   = @($enriched | Where-Object { $_.Email -eq "" })

$withEmail | Export-Csv -Path $withEmailCsv -NoTypeInformation -Encoding UTF8
$noEmail   | Export-Csv -Path $noEmailCsv   -NoTypeInformation -Encoding UTF8

if (Test-Path $checkpointCsv) { Remove-Item $checkpointCsv -Force }

$totalMins = [math]::Round(((Get-Date)-$pipelineStart).TotalMinutes,1)
$matchPct  = [math]::Round($matched/[math]::Max($counter,1)*100)

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Pipeline Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Total runtime   : $totalMins min"
Write-Host "Authors pulled  : $($allAuthors.Count)"
Write-Host "Emails found    : $($withEmail.Count) ($matchPct%)" -ForegroundColor Green
Write-Host "No email        : $($noEmail.Count)"
Write-Host ""
Write-Host "Output files:"
Write-Host "  $withEmailCsv" -ForegroundColor Green
Write-Host "  $noEmailCsv"   -ForegroundColor Yellow
Write-Host ""
Write-Host "Sample with emails:"
$withEmail | Select-Object -First 10 | Format-Table Author, Institution, Email, Email_Source -AutoSize
