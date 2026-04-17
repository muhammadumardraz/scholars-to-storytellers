param(
    [Parameter(Mandatory=$true)]
    [string]$Department,
    [string]$WorkDir                = "$env:USERPROFILE\Downloads",
    [string]$EmailForAPI            = "pakgeniusatwork@gmail.com",
    [int]$MinWorksCount             = 3,
    [int]$MinCitedBy                = 0,
    [int]$DelayMs                   = 500,
    [int]$BookBatch                 = 30,
    [int]$EmailDelayMs              = 400,
    [int]$MinCitationsWorks         = 0,
    # Comma-separated keywords: author must have at least one in their concepts
    [string]$RequiredTopicKeywords  = ""
)

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
[Net.ServicePointManager]::Expect100Continue = $false

$amp = [char]38

# OpenAlex subfield IDs (ASJC-based):
#   1202 = History                              1208 = Literature and Literary Theory
#   1212 = Religious Studies                    3305 = Geography
#   3312 = Sociology                            3314 = Anthropology
#   3315 = Communication                        3316 = Cultural Studies
#   3320 = Political Science & Int'l Relations
#
# Interdisciplinary fields (no dedicated subfield) use closest pool + RequiredTopicKeywords filter
$subfields = @{
    "Anthropology"               = "3314"
    "History"                    = "1202"
    "Sociology"                  = "3312"
    "American Studies"           = "1202"   # History pool — filtered by US-focus keywords
    "African American Studies"   = "3316"   # Cultural Studies pool — filtered by race/diaspora keywords
    "Asian American Studies"     = "3312"   # Sociology pool — filtered by Asian diaspora keywords
    "Global Studies"             = "3320"   # Political Science & International Relations
    "English"                    = "1208"   # Literature and Literary Theory
    "Jewish Studies"             = "1212"   # Religious Studies
    "Cultural Studies"           = "3316"
    "Communication/Media"        = "3315"
    "Geography"                  = "3305"
}

# Parse required topic keywords (OR logic — author must match at least one)
$requiredKeywords = @()
if ($RequiredTopicKeywords.Trim() -ne "") {
    $requiredKeywords = $RequiredTopicKeywords -split "," | ForEach-Object { $_.Trim().ToLower() } | Where-Object { $_ }
}

if (-not $subfields.ContainsKey($Department)) {
    Write-Host "Unknown department: $Department" -ForegroundColor Red
    Write-Host "Valid options: $($subfields.Keys -join ', ')"
    exit 1
}

$sfId    = $subfields[$Department]
$sfSafe  = $Department -replace '[^a-zA-Z0-9]','_'

# Citation floor: 0 for most departments, caller can override (e.g. Anthropology uses 50)
$minCitations = $MinCitationsWorks
$maxScanPages = 200

$withEmailCsv = "$WorkDir\SOP_With_Emails.csv"
$noEmailCsv   = "$WorkDir\SOP_No_Email.csv"
$ckFile       = "$WorkDir\SOP_CK_${sfSafe}.csv"
$cursorFile   = "$WorkDir\SOP_CK_${sfSafe}_cursor.txt"
$p3CacheFile  = "$WorkDir\SOP_P3_${sfSafe}.csv"   # Phase 3 cache for resume

$hardSciList = @("medicine","biology","chemistry","physics","mathematics",
                 "computer science","engineering","neuroscience","genomics",
                 "ecology","geology","machine learning","astronomy","statistics",
                 "environmental science","earth science","materials science",
                 "biochemistry","molecular","genetics","immunology","pharmacology",
                 "climate","atmospheric","oceanography","hydrology","geophysics",
                 "botany","zoology","microbiology","neurology","cardiology",
                 # Business / law / clinical — not our target
                 "business administration","management science","finance","accounting",
                 "marketing","operations research","information systems",
                 "jurisprudence","criminal justice","criminology",
                 "nursing","dentistry","veterinary","pharmacy","public health")

$badInstKeywords = @(
    # Tech companies
    "twitter","facebook","google","microsoft","amazon","apple",
    "illumina","linkedin","salesforce","oracle","ibm","intel",
    "qualcomm","nvidia","uber","airbnb","spotify","netflix",
    # Think tanks & policy orgs (not academic)
    "think tank","resources for the future","rand corporation",
    "brookings","heritage foundation","cato institute",
    "american enterprise institute","hoover institution",
    # Military academies
    "naval academy","military academy","air force academy",
    "war college","national defense university","coast guard academy",
    # Standalone hospitals / medical centers (not university-affiliated)
    "memorial hospital","general hospital","veterans affairs",
    "mayo clinic","cleveland clinic","kaiser permanente"
)

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

$apiHeaders = @{ "User-Agent" = "ScholarsToStorytellers/1.0 (mailto:$EmailForAPI)" }

function Invoke-OA([string]$Url) {
    $delay429 = @(120, 300, 600)
    $delayErr = @(5, 15, 45)
    for ($i = 0; $i -lt 4; $i++) {
        try {
            return Invoke-RestMethod -Uri $Url -Headers $script:apiHeaders -Method Get -TimeoutSec 45 -ErrorAction Stop
        } catch {
            $err   = $_.Exception.Message
            $is429 = $err -like "*429*" -or $err -like "*Too Many*"
            if ($i -lt 3) {
                $wait = if ($is429) { $delay429[$i] } else { $delayErr[$i] }
                $tag  = if ($is429) { "rate-limited (429)" } else { "error" }
                Write-Host "    [API] Attempt $($i+1) $tag - waiting ${wait}s..." -ForegroundColor DarkYellow
                Start-Sleep -Seconds $wait
            } else {
                Write-Host "    [API] All retries exhausted, skipping." -ForegroundColor Red
            }
        }
    }
    return $null
}

function Fetch-Page([string]$url) {
    if (-not $url -or $url.Trim() -eq "") { return $null }
    $agents = @(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    )
    try {
        return (Invoke-WebRequest -Uri $url -TimeoutSec 14 -UseBasicParsing -UserAgent ($agents | Get-Random) -ErrorAction Stop).Content
    } catch { return $null }
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

$skipDomains = @('sentry','example','noreply','no-reply','support','webmaster','admin',
                 'info','test','placeholder','youremail','privacy','abuse','help',
                 'feedback','contact','press','media','doi','crossref','elsevier',
                 'springer','wiley','tandfonline')
$emailRegex = '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'

function Get-BestEmail([string]$text, [string]$preferDomain, [string]$firstName, [string]$lastName) {
    if (-not $text) { return $null }
    $raw = [System.Collections.ArrayList]::new()
    foreach ($m in [regex]::Matches($text, $emailRegex)) { [void]$raw.Add(@{ email=$m.Value.Trim().ToLower(); idx=$m.Index }) }
    $clean = $raw | Where-Object {
        $ext=$($_.email -split '\.')[-1]; $dom=$($_.email -split '@')[-1]; $loc=$($_.email -split '@')[0]
        @('png','jpg','gif','svg','css','js','ico') -notcontains $ext -and
        ($skipDomains | Where-Object { $dom -like "*$_*" }).Count -eq 0 -and
        $loc.Length -gt 1 -and $dom.Length -gt 3
    }
    if (-not $clean -or @($clean).Count -eq 0) { return $null }
    $lastLower = $lastName.ToLower(); $window = 600; $textLower = $text.ToLower()
    $nearName = [System.Collections.ArrayList]::new()
    foreach ($c in $clean) {
        $pos = 0
        while ($true) {
            $np = $textLower.IndexOf($lastLower, $pos); if ($np -lt 0) { break }
            if ([math]::Abs($c.idx - $np) -le $window) { [void]$nearName.Add($c); break }
            $pos = $np + 1
        }
    }
    $last4 = $lastLower.Substring(0,[math]::Min(4,$lastLower.Length))
    $nameInLocal = @($clean | Where-Object { $loc=($_.email -split '@')[0]; $loc -like "*$lastLower*" -or $loc -like "*$last4*" })
    $nameInst = @($nameInLocal | Where-Object { $preferDomain -and $_.email -like "*@*$preferDomain*" })
    $nameEdu  = @($nameInLocal | Where-Object { $_.email -like "*.edu" -or $_.email -like "*.ac.*" })
    $nameAny  = @($nameInLocal | Where-Object { $_.email -notlike "*gmail*" -and $_.email -notlike "*yahoo*" -and $_.email -notlike "*hotmail*" })
    $nearInst = @($nearName   | Where-Object { $preferDomain -and $_.email -like "*@*$preferDomain*" })
    $nearEdu  = @($nearName   | Where-Object { $_.email -like "*.edu" -or $_.email -like "*.ac.*" })
    if ($nameInst.Count -gt 0) { return @{ email=$nameInst[0].email; confidence="strict" } }
    if ($nameEdu.Count  -gt 0) { return @{ email=$nameEdu[0].email;  confidence="strict" } }
    if ($nameAny.Count  -gt 0) { return @{ email=$nameAny[0].email;  confidence="medium" } }
    if ($nearInst.Count -gt 0) { return @{ email=$nearInst[0].email; confidence="loose-inst" } }
    if ($nearEdu.Count  -gt 0) { return @{ email=$nearEdu[0].email;  confidence="loose-edu" } }
    return $null
}

function Name-IsOnPage([string]$content,[string]$lastName) {
    if (-not $content) { return $false }
    return $content.ToLower() -match [regex]::Escape($lastName.ToLower())
}

function Get-NameVariants([string]$fullName) {
    $p = $fullName.Trim() -split "\s+"
    return @{ full=$fullName.Trim(); short="$($p[0]) $($p[-1])"; first=$p[0]; last=$p[-1] }
}

function Get-AuthorPageUrl([string]$oaId) {
    $short = $oaId -replace "https://openalex.org/",""
    try { $r = Invoke-RestMethod -Uri "https://api.openalex.org/authors/$short" -Method Get -TimeoutSec 15 -ErrorAction Stop; return $r.homepage_url } catch { return $null }
}

function Get-SopWorks([string]$oaId) {
    $short = $oaId -replace "https://openalex.org/",""
    $url = "https://api.openalex.org/works?filter=author.id:$short,type:article$($amp)sort=cited_by_count:desc$($amp)per_page=5$($amp)select=id,doi,best_oa_location,primary_location"
    try { $r = Invoke-RestMethod -Uri $url -Method Get -TimeoutSec 15 -ErrorAction Stop; return $r.results } catch { return @() }
}

function Find-Email($row) {
    $domain = $row.Institution_Domain -replace "https?://(www\.)?","" -replace "/.*",""
    $authorName = if ($row.Name) { $row.Name } elseif ($row.Author) { $row.Author } else { "" }
    $n = Get-NameVariants $authorName
    $fn=$n.first; $ln=$n.last; $full=$n.full; $short=$n.short
    $pad="    "
    $result = @{ email=""; source=""; homepage="" }

    Write-Host "$pad [1/6] OpenAlex homepage..." -ForegroundColor DarkGray
    $homepage = Get-AuthorPageUrl $row.OpenAlex_ID
    Start-Sleep -Milliseconds 250
    if ($homepage) {
        $result.homepage = $homepage
        $c = Fetch-Page $homepage
        if ($c -and (Name-IsOnPage $c $ln)) {
            $f = Get-BestEmail $c $domain $fn $ln
            if ($f -and $f.confidence -notlike "loose*") { $result.email=$f.email; $result.source="faculty page ($($f.confidence))"; return $result }
        }
        Start-Sleep -Milliseconds 200
    }

    Write-Host "$pad [2/6] SOP article pages..." -ForegroundColor DarkGray
    $works = Get-SopWorks $row.OpenAlex_ID
    Start-Sleep -Milliseconds 250
    foreach ($work in $works) {
        $urls = @()
        if ($work.best_oa_location -and $work.best_oa_location.landing_page_url) { $urls += $work.best_oa_location.landing_page_url }
        if ($work.primary_location -and $work.primary_location.landing_page_url -and $urls -notcontains $work.primary_location.landing_page_url) { $urls += $work.primary_location.landing_page_url }
        foreach ($url in $urls) {
            $c = Fetch-Page $url
            if (-not $c) { continue }
            $f = Get-BestEmail $c $domain $fn $ln
            if ($f -and $f.confidence -eq "strict") { $result.email=$f.email; $result.source="paper page ($($f.confidence))"; return $result }
            Start-Sleep -Milliseconds 200
        }
    }

    Write-Host "$pad [3/6] Google Scholar..." -ForegroundColor DarkGray
    foreach ($q in @([uri]::EscapeDataString("$full $($row.Institution)"), [uri]::EscapeDataString($short)) | Select-Object -Unique) {
        $c = Fetch-Page "https://scholar.google.com/scholar?q=$q&hl=en"
        if ($c) {
            $m = [regex]::Match($c, 'href="(/citations\?user=[^"&]+)')
            if ($m.Success) {
                $sp = Fetch-Page "https://scholar.google.com$($m.Groups[1].Value)"
                if ($sp -and (Name-IsOnPage $sp $ln)) {
                    $hm = [regex]::Match($sp, 'href="(https?://(?!scholar\.google)[^"]+)"[^>]*>Homepage')
                    if ($hm.Success) {
                        $hc = Fetch-Page $hm.Groups[1].Value
                        if ($hc -and (Name-IsOnPage $hc $ln)) {
                            $f = Get-BestEmail $hc $domain $fn $ln
                            if ($f -and $f.confidence -notlike "loose*") { $result.email=$f.email; $result.source="Scholar->homepage ($($f.confidence))"; return $result }
                        }
                    }
                    $f = Get-BestEmail $sp $domain $fn $ln
                    if ($f -and $f.confidence -notlike "loose*") { $result.email=$f.email; $result.source="Google Scholar ($($f.confidence))"; return $result }
                }
                break
            }
        }
        Start-Sleep -Milliseconds 400
    }

    Write-Host "$pad [4/6] ORCID..." -ForegroundColor DarkGray
    if ($row.ORCID -and $row.ORCID -ne "") {
        $oc = Fetch-Page "https://pub.orcid.org/v3.0/$($row.ORCID -replace 'https://orcid.org/','')/emails"
        if ($oc) {
            $f = Get-BestEmail $oc $domain $fn $ln
            if ($f -and $f.confidence -notlike "loose*") { $result.email=$f.email; $result.source="ORCID ($($f.confidence))"; return $result }
        }
        Start-Sleep -Milliseconds 200
    }

    Write-Host "$pad [5/6] University directory..." -ForegroundColor DarkGray
    if ($domain) {
        $encShort = [uri]::EscapeDataString($short)
        foreach ($url in @("https://$domain/directory?search=$encShort","https://$domain/people?search=$encShort","https://$domain/faculty?search=$encShort") | Select-Object -Unique) {
            $c = Fetch-Page $url
            if ($c -and (Name-IsOnPage $c $ln)) {
                $f = Get-BestEmail $c $domain $fn $ln
                if ($f -and $f.confidence -notlike "loose*") { $result.email=$f.email; $result.source="university directory ($($f.confidence))"; return $result }
            }
            Start-Sleep -Milliseconds 150
        }
    }

    Write-Host "$pad [6/6] ResearchGate..." -ForegroundColor DarkGray
    foreach ($url in @("https://www.researchgate.net/profile/$fn-$ln","https://www.researchgate.net/profile/$fn-$ln-1") | Select-Object -Unique) {
        $c = Fetch-Page $url
        if ($c -and (Name-IsOnPage $c $ln)) {
            $f = Get-BestEmail $c $domain $fn $ln
            if ($f -and $f.confidence -notlike "loose*") { $result.email=$f.email; $result.source="ResearchGate ($($f.confidence))"; return $result }
            break
        }
        Start-Sleep -Milliseconds 300
    }

    return $result
}

# =============================================================================
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  S2S - Department Runner: $Department" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
$start = Get-Date

# =============================================================================
# RESUME CHECK: If Phase 3 cache exists, skip Phases 1-3 entirely
# =============================================================================
if (Test-Path $p3CacheFile) {
    Write-Host ""
    Write-Host "  [RESUME] Phase 3 cache found - skipping Phases 1-3" -ForegroundColor Yellow
    $rows = @(Import-Csv $p3CacheFile)
    Write-Host "  Loaded $($rows.Count) authors from cache" -ForegroundColor Green

} else {
    # =============================================================================
    # PHASE 1 - Query Works API to collect author IDs
    # =============================================================================
    Write-Host "  [Phase 1] Querying authors from OpenAlex..." -ForegroundColor Cyan
    $authorData = @{}
    $cursor = "*"; $pageCount = 0

    if ((Test-Path $ckFile) -and (Test-Path $cursorFile)) {
        foreach ($r in (Import-Csv $ckFile)) {
            $authorData[$r.Id] = @{
                Id=$r.Id; DisplayName=$r.DisplayName; ORCID=$r.ORCID
                Institution=$r.Institution; Field=$Department; Topics=$r.Topics
                WorksCount=[int]$r.WorksCount; CitedBy=[int]$r.CitedBy
                HasAcademicBook=$false; HasTradeBook=$false
                AcademicBooks=[System.Collections.ArrayList]::new()
            }
        }
        $cursor = (Get-Content $cursorFile -Raw).Trim()
        Write-Host "  Resuming from checkpoint: $($authorData.Count) authors" -ForegroundColor Yellow
    }

    $authorFieldMap = @{}
    $worksScanned = 0

    while ($true) {
        $citFilter = if ($minCitations -gt 0) { ",cited_by_count:>$minCitations" } else { "" }
        $url = "https://api.openalex.org/works?" +
               "filter=institutions.country_code:US,primary_topic.subfield.id:$sfId$citFilter" +
               $amp + "sort=cited_by_count:desc" +
               $amp + "per_page=200" +
               $amp + "cursor=" + [uri]::EscapeDataString($cursor) +
               $amp + "select=id,authorships"
        $data = Invoke-OA $url
        Start-Sleep -Milliseconds $DelayMs
        if (-not $data -or -not $data.results -or $data.results.Count -eq 0) { break }
        if ($pageCount -ge $maxScanPages) { Write-Host "    [Page cap $maxScanPages reached]" -ForegroundColor DarkYellow; break }

        foreach ($work in $data.results) {
            foreach ($auth in $work.authorships) {
                if (-not $auth.author -or -not $auth.author.id) { continue }
                $hasUS = $false
                foreach ($inst in $auth.institutions) {
                    if ($inst.country_code -eq "US" -and $inst.type -in @("education","nonprofit")) { $hasUS = $true; break }
                }
                if (-not $hasUS) { continue }
                $aId = $auth.author.id -replace "https://openalex.org/",""
                if (-not $authorFieldMap.ContainsKey($aId)) { $authorFieldMap[$aId] = $Department }
            }
        }
        $worksScanned += $data.results.Count
        $pageCount++
        Write-Host "    -> page $pageCount | $worksScanned works | $($authorFieldMap.Count) authors" -ForegroundColor DarkGray

        if ($pageCount % 5 -eq 0) {
            $authorFieldMap.Keys | ForEach-Object { [PSCustomObject]@{ AuthorId=$_; Field=$authorFieldMap[$_] } } |
                Export-Csv -Path $ckFile -NoTypeInformation -Encoding UTF8
            Set-Content -Path $cursorFile -Value $cursor -Encoding UTF8
            Write-Host "    [CHECKPOINT saved]" -ForegroundColor DarkGreen
        }
        if ($data.meta -and $data.meta.next_cursor -and $data.meta.next_cursor -ne $cursor) { $cursor = $data.meta.next_cursor } else { break }
        if ($data.results.Count -lt 200) { break }
    }
    if (Test-Path $ckFile)    { Remove-Item $ckFile    -Force }
    if (Test-Path $cursorFile){ Remove-Item $cursorFile -Force }
    Write-Host "  [Phase 1] DONE: $worksScanned works -> $($authorFieldMap.Count) author IDs" -ForegroundColor Green

    if ($authorFieldMap.Count -eq 0) { Write-Host "No authors found - check network/API." -ForegroundColor Red; exit 1 }

    # =============================================================================
    # Phase 2a: Enrich authors
    # =============================================================================
    Write-Host "  [Phase 2a] Enriching authors..." -ForegroundColor Cyan
    $authorData = @{}
    $chunks = Split-Chunks @($authorFieldMap.Keys) 50
    $cc = 0
    foreach ($chunk in $chunks) {
        $cc++
        $url = "https://api.openalex.org/authors?filter=openalex_id:" + ($chunk -join "|") +
               $amp + "select=id,display_name,ids,last_known_institutions,works_count,cited_by_count,x_concepts" +
               $amp + "per_page=50"
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
                # Build full concept list for filtering; store top 4 for output
                $allConcepts = if ($a.x_concepts) { $a.x_concepts | ForEach-Object { $_.display_name } } else { @() }
                $topics = ($allConcepts | Select-Object -First 4) -join "; "
                $topicsAllLower = ($allConcepts -join " ").ToLower()

                # Filter out hard-science / unwanted-discipline authors
                $isHard = $false
                foreach ($topicPart in ($allConcepts)) {
                    $t = $topicPart.Trim().ToLower()
                    foreach ($hs in $hardSciList) { if ($t -like "*$hs*") { $isHard = $true; break } }
                    if ($isHard) { break }
                }
                if ($isHard) { continue }

                # For interdisciplinary departments: require at least one matching keyword
                if ($requiredKeywords.Count -gt 0) {
                    $hasRequired = $false
                    foreach ($kw in $requiredKeywords) {
                        if ($topicsAllLower -like "*$kw*") { $hasRequired = $true; break }
                    }
                    if (-not $hasRequired) { continue }
                }
                $authorData[$aShort] = @{
                    Id=$aShort; DisplayName=$a.display_name
                    ORCID=if ($a.ids -and $a.ids.orcid) { $a.ids.orcid } else { "" }
                    Institution=$usInst.display_name; Field=$Department; Topics=$topics
                    WorksCount=$a.works_count; CitedBy=$a.cited_by_count
                    HasAcademicBook=$false; HasTradeBook=$false
                    AcademicBooks=[System.Collections.ArrayList]::new()
                }
            }
        }
        if ($cc % 20 -eq 0) { Write-Host "    [$cc/$($chunks.Count)] Qualified: $($authorData.Count)" }
    }
    Write-Host "  [Phase 2a] $($authorData.Count) authors qualified" -ForegroundColor Green

    if ($authorData.Count -eq 0) { Write-Host "No qualified authors found." -ForegroundColor Red; exit 1 }

    # =============================================================================
    # Phase 2b: Book classification
    # =============================================================================
    Write-Host "  [Phase 2b] Classifying books (20s cooldown)..." -ForegroundColor Cyan
    Start-Sleep -Seconds 20

    $bookChunks = Split-Chunks @($authorData.Keys) $BookBatch
    $bc = 0
    foreach ($chunk in $bookChunks) {
        $bc++
        $url  = "https://api.openalex.org/works?filter=author.id:" + ($chunk -join "|") + ",type:book|book-chapter" +
                $amp + "select=id,title,publication_year,primary_location,authorships" + $amp + "per_page=100"
        $resp = Invoke-OA $url
        Start-Sleep -Milliseconds 1200
        if (-not $resp -or -not $resp.results) { continue }
        foreach ($book in $resp.results) {
            $pubName = ""
            if ($book.primary_location -and $book.primary_location.source) {
                $src = $book.primary_location.source
                $pubName = if ($src.host_organization_name) { $src.host_organization_name } elseif ($src.display_name) { $src.display_name } else { "" }
            }
            $pubLower = $pubName.ToLower(); $isTrade = $false
            foreach ($tp in $tradePublishers) { if ($pubLower -like "*$tp*") { $isTrade = $true; break } }
            foreach ($auth in $book.authorships) {
                if (-not $auth.author -or -not $auth.author.id) { continue }
                $aShort = $auth.author.id -replace "https://openalex.org/",""
                if (-not $authorData.ContainsKey($aShort)) { continue }
                if ($isTrade) { $authorData[$aShort].HasTradeBook = $true }
                else {
                    $authorData[$aShort].HasAcademicBook = $true
                    $label = if ($book.title -and $book.publication_year) { "$($book.title) ($($book.publication_year))" } else { $book.title }
                    if ($label -and $authorData[$aShort].AcademicBooks -notcontains $label) { [void]$authorData[$aShort].AcademicBooks.Add($label) }
                }
            }
        }
        if ($bc % 50 -eq 0) { Write-Host "    [$bc/$($bookChunks.Count)] chunks done" -ForegroundColor DarkGray }
    }
    $withAcad = ($authorData.Values | Where-Object { $_.HasAcademicBook -and -not $_.HasTradeBook }).Count
    $noBooks  = ($authorData.Values | Where-Object { -not $_.HasAcademicBook -and -not $_.HasTradeBook }).Count
    $trade    = ($authorData.Values | Where-Object { $_.HasTradeBook }).Count
    Write-Host "  [Phase 2] Academic books: $withAcad | No books yet: $noBooks (prime targets) | Trade excluded: $trade" -ForegroundColor Green

    # =============================================================================
    # PHASE 3 - Domain resolution
    # =============================================================================
    Write-Host "  [Phase 3] Resolving institution domains..." -ForegroundColor Cyan
    $uniqueInsts = @($authorData.Values | Where-Object { -not $_.HasTradeBook } | ForEach-Object { $_.Institution } | Select-Object -Unique | Where-Object { $_ })
    $domainCache = @{}
    foreach ($instName in $uniqueInsts) {
        try {
            $r = Invoke-RestMethod -Uri "https://api.ror.org/organizations?affiliation=$([uri]::EscapeDataString($instName))" -Method Get -TimeoutSec 15 -ErrorAction Stop
            if ($r.items -and $r.items.Count -gt 0) {
                $org = ($r.items | Sort-Object score -Descending | Select-Object -First 1).organization
                if ($org -and $org.links -and $org.links.Count -gt 0) {
                    $link = $org.links | Where-Object { $_.type -eq "website" } | Select-Object -First 1
                    if (-not $link) { $link = $org.links[0] }
                    $siteUrl = if ($link -is [string]) { $link } elseif ($link.value) { $link.value } else { "" }
                    $domain = ($siteUrl -replace "https?://(www\.)?","" -replace "/.*","").Trim().ToLower()
                    if ($domain) { $domainCache[$instName] = $domain }
                }
            }
        } catch {}
        if (-not $domainCache.ContainsKey($instName)) { $domainCache[$instName] = "" }
        Start-Sleep -Milliseconds 200
    }
    Write-Host "  [Phase 3] $($domainCache.Values | Where-Object { $_ }) domains resolved" -ForegroundColor Green

    # Build rows (all authors without a trade book)
    $rows = @($authorData.Values | Where-Object { -not $_.HasTradeBook } | ForEach-Object {
        $d = $_
        [PSCustomObject]@{
            Name               = $d.DisplayName
            Department         = $d.Field
            Institution        = $d.Institution
            Institution_Domain = if ($domainCache.ContainsKey($d.Institution)) { $domainCache[$d.Institution] } else { "" }
            ORCID              = $d.ORCID
            Research_Topics    = $d.Topics
            Book_Status        = if ($d.HasAcademicBook) { "Academic books" } else { "No books yet" }
            Academic_Books     = if ($d.AcademicBooks.Count -gt 0) { $d.AcademicBooks -join " | " } else { "" }
            Works_Count        = $d.WorksCount
            Cited_By_Count     = $d.CitedBy
            OpenAlex_ID        = "https://openalex.org/" + $d.Id
            OpenAlex_Profile   = "https://openalex.org/" + $d.Id
        }
    } | Sort-Object Cited_By_Count -Descending)

    Write-Host "  $($rows.Count) authors qualify for email enrichment" -ForegroundColor Green

    # Save Phase 3 cache so future batches skip straight to Phase 4
    $rows | Export-Csv -Path $p3CacheFile -NoTypeInformation -Encoding UTF8
    Write-Host "  [Phase 3 cache saved: $p3CacheFile]" -ForegroundColor DarkGreen

} # end if/else p3CacheFile

# =============================================================================
# PHASE 4 - Email enrichment (runs every batch, resumes from where it left off)
# =============================================================================
Write-Host "  [Phase 4] Email enrichment..." -ForegroundColor Cyan

# Load already-processed author IDs from any previous runs/batches
$doneIds = @{}
foreach ($csvPath in @($withEmailCsv, $noEmailCsv)) {
    if (Test-Path $csvPath) {
        foreach ($r in (Import-Csv $csvPath)) {
            if ($r.OpenAlex_ID) { $doneIds[$r.OpenAlex_ID] = $true }
        }
    }
}

if ($doneIds.Count -gt 0) {
    $remaining = $rows.Count - $doneIds.Count
    Write-Host "  Resuming: $($doneIds.Count) already done, $remaining remaining" -ForegroundColor Yellow
}

# Build the list of authors still needing processing
$toProcess = @($rows | Where-Object { -not $doneIds.ContainsKey($_.OpenAlex_ID) })

if ($toProcess.Count -eq 0) {
    Write-Host "  All $($rows.Count) authors already processed - nothing to do!" -ForegroundColor Green
    $totalMins = [math]::Round(((Get-Date)-$start).TotalMinutes,1)
    Write-Host "  [Completed in ${totalMins}min]" -ForegroundColor Cyan
    exit 0
}

Write-Host "  Processing $($toProcess.Count) authors this batch..." -ForegroundColor Cyan
Write-Host ""
$counter=0; $matched=0

foreach ($row in $toProcess) {
    $counter++
    Write-Host "[$counter/$($toProcess.Count)] $($row.Name) @ $($row.Institution)" -ForegroundColor White
    $res = Find-Email $row

    if ($res.email) { $matched++; Write-Host "  => FOUND: $($res.email) [$($res.source)]" -ForegroundColor Green }
    else            { Write-Host "  => no email" -ForegroundColor Yellow }

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

    if ($res.email) { $outRow | Export-Csv -Path $withEmailCsv -Append -NoTypeInformation -Encoding UTF8 }
    else            { $outRow | Export-Csv -Path $noEmailCsv   -Append -NoTypeInformation -Encoding UTF8 }

    if ($counter % 5 -eq 0) {
        $pct  = [math]::Round($matched/[math]::Max($counter,1)*100)
        $mins = [math]::Round(((Get-Date)-$start).TotalMinutes,1)
        $totalDoneNow = $doneIds.Count + $counter
        Write-Host ""
        Write-Host "  --- [$mins min] batch: $counter/$($toProcess.Count) | total: $totalDoneNow/$($rows.Count) | $matched emails ($pct%) ---" -ForegroundColor Cyan
        Write-Host ""
    }
    Start-Sleep -Milliseconds $EmailDelayMs
}

$totalMins  = [math]::Round(((Get-Date)-$start).TotalMinutes,1)
$pct        = [math]::Round($matched/[math]::Max($counter,1)*100)
$grandTotal = $doneIds.Count + $counter

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  $Department - Batch Complete" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Runtime (this batch) : $totalMins min"
Write-Host "Processed this batch : $counter"
Write-Host "Emails this batch    : $matched ($pct%)" -ForegroundColor Green
Write-Host "Grand total          : $grandTotal / $($rows.Count)" -ForegroundColor Green
if ($grandTotal -ge $rows.Count) {
    Write-Host "  *** DEPARTMENT FULLY COMPLETE ***" -ForegroundColor Green
}
Write-Host "Output: $withEmailCsv" -ForegroundColor Green
