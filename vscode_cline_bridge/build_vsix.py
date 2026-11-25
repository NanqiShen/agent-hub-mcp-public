"""Package vscode_cline_bridge into a standard .vsix archive using stdlib zipfile."""

from __future__ import annotations

import json
from pathlib import Path
import zipfile


CONTENT_TYPES_XML = """<?xml version="1.0" encoding="utf-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension=".json" ContentType="application/json"/>
  <Default Extension=".js" ContentType="application/javascript"/>
  <Default Extension=".vsixmanifest" ContentType="text/xml"/>
</Types>
"""


def build_manifest(pkg: dict) -> str:
    publisher = pkg["publisher"]
    name = pkg["name"]
    version = pkg["version"]
    display = pkg.get("displayName", name)
    desc = pkg.get("description", "")
    return f"""<?xml version="1.0" encoding="utf-8"?>
<PackageManifest Version="2.0.0" xmlns="http://schemas.microsoft.com/developer/vsx-schema/2011">
  <Metadata>
    <Identity Language="en-US" Id="{name}" Version="{version}" Publisher="{publisher}" />
    <DisplayName>{display}</DisplayName>
    <Description xml:space="preserve">{desc}</Description>
    <Categories>Other</Categories>
  </Metadata>
  <Installation>
    <InstallationTarget Id="Microsoft.VisualStudio.Code" />
  </Installation>
  <Dependencies />
  <Assets>
    <Asset Type="Microsoft.VisualStudio.Code.Manifest" Path="extension/package.json" Addressable="true" />
  </Assets>
</PackageManifest>
"""


def main() -> Path:
    root = Path(__file__).resolve().parent
    pkg_path = root / "package.json"
    ext_path = root / "extension.js"
    pkg = json.loads(pkg_path.read_text(encoding="utf-8"))
    out_vsix = root / f"{pkg['name']}-{pkg['version']}.vsix"

    with zipfile.ZipFile(out_vsix, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", CONTENT_TYPES_XML)
        zf.writestr("extension.vsixmanifest", build_manifest(pkg))
        zf.write(pkg_path, "extension/package.json")
        zf.write(ext_path, "extension/extension.js")

    print(f"Built {out_vsix} ({out_vsix.stat().st_size} bytes)")
    return out_vsix


if __name__ == "__main__":
    main()
