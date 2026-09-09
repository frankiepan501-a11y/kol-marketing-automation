# Preserve unresolved review notes when relocking

Relocking a new-outreach participant rebuilt review fields and cleared pending-review notes. The existing preservation check protected only approved reviews. Sorting and refreshed profile data do not prove that a previously recorded evidence gap was resolved.

The change preserves the decision, reason, reviewer and review time for existing new-outreach participants with a nonempty reason and a pending-review or needs-information decision. Approved reviews still require unchanged country, language and profile. Other ranking fields continue to refresh; no sending rule is relaxed.

Changed: `app/launch_participation.py`; regression coverage in `tests/test_launch_participation.py`. Validation: 25 participation tests passed; independent review found no blocking regression. No production relock or email was triggered.

Deployment: not deployed. Zeabur deployment-target verification returned HTTP 401 with the currently configured API credential on 2026-09-09. Restore valid platform authentication and verify the live deployment before release; do not treat HTTP 200 on the public health endpoint as version proof. The defect is reproducible from source; the exact writer responsible for the observed cleared records has not been proven from production logs.
