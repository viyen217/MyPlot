<?php
declare(strict_types=1);
namespace MyPlot\provider;

use MyPlot\MyPlot;
use MyPlot\Plot;
use poggit\libasynql\libasynql;

class SQLiteDataProvider extends DataProvider
{
	/** @var string $db */
	private $db;
	/** @var string $sqlGetPlot */
	protected $sqlGetPlot = "SELECT id, name, owner, helpers, denied, biome, pvp FROM plots WHERE level = :level AND X = :X AND Z = :Z;";
	/** @var string $sqlSavePlot */
	protected $sqlSavePlot = "INSERT OR REPLACE INTO plots (id, level, X, Z, name, owner, helpers, denied, biome, pvp) VALUES
			((SELECT id FROM plots WHERE level = :level AND X = :X AND Z = :Z),
			 :level, :X, :Z, :name, :owner, :helpers, :denied, :biome, :pvp);";
	/** @var string $sqlSavePlotById */
	protected $sqlSavePlotById = "UPDATE plots SET name = :name, owner = :owner, helpers = :helpers, denied = :denied, biome = :biome, pvp = :pvp WHERE id = :id;";
	/** @var string $sqlRemovePlot */
	protected $sqlRemovePlot = "DELETE FROM plots WHERE level = :level AND X = :X AND Z = :Z;";
	/** @var string $sqlRemovePlotById */
	protected $sqlRemovePlotById = "DELETE FROM plots WHERE id = :id;";
	/** @var string $sqlGetPlotsByOwner */
	protected $sqlGetPlotsByOwner = "SELECT * FROM plots WHERE owner = :owner;";
	/** @var string $sqlGetPlotsByOwnerAndLevel */
	protected $sqlGetPlotsByOwnerAndLevel = "SELECT * FROM plots WHERE owner = :owner AND level = :level;";
	/** @var string $sqlGetExistingXZ */
	protected $sqlGetExistingXZ = "SELECT X, Z FROM plots WHERE (
				level = :level
				AND (
					(abs(X) = :number AND abs(Z) <= :number) OR
					(abs(Z) = :number AND abs(X) <= :number)
				)
			);";

	/**
	 * SQLiteDataProvider constructor.
	 *
	 * @param MyPlot $plugin
	 * @param int $cacheSize
	 */
	public function __construct(MyPlot $plugin, int $cacheSize = 0) {
		parent::__construct($plugin, $cacheSize);
		$this->db = libasynql::create($plugin, [
			"type" => "sqlite3",
			"sqlite" => [
				"file" => "plots.db"
			],
			"worker-limit" => 2
		], [
			"sqlite3" => []
		]);
		$this->db->executeGenericRaw("CREATE TABLE IF NOT EXISTS plots
			(id INTEGER PRIMARY KEY AUTOINCREMENT, level TEXT, X INTEGER, Z INTEGER, name TEXT,
			 owner TEXT, helpers TEXT, denied TEXT, biome TEXT, pvp INTEGER);");
		$this->db->executeGenericRaw("ALTER TABLE plots ADD pvp INTEGER;");
		$this->plugin->getLogger()->debug("SQLite data provider registered");
	}

	/**
	 * @param Plot $plot
	 *
	 * @return bool
	 */
	public function savePlot(Plot $plot) : bool {
		$helpers = implode(',', $plot->helpers);
		$denied = implode(',', $plot->denied);
		if($plot->id >= 0) {
			$this->db->executeChangeRaw($this->sqlSavePlotById, [":id" => $plot->id, ":level" => $plot->levelName, ":X" => $plot->X, ":Z" => $plot->Z, ":name" => $plot->name, ":owner" => $plot->owner, ":helpers" => $helpers, ":denied" => $denied, ":biome" => $plot->biome, ":pvp" => $plot->pvp]);
		}else{
			$this->db->executeInsertRaw($this->sqlSavePlot, [":level" => $plot->levelName, ":X" => $plot->X, ":Z" => $plot->Z, ":name" => $plot->name, ":owner" => $plot->owner, ":helpers" => $helpers, ":denied" => $denied, ":biome" => $plot->biome, ":pvp" => $plot->pvp]);
		}
		$this->cachePlot($plot);
		return true;
	}

	/**
	 * @param Plot $plot
	 *
	 * @return bool
	 */
	public function deletePlot(Plot $plot) : bool {
		if($plot->id >= 0) {
			$this->db->executeChangeRaw($this->sqlRemovePlot, [":id" => $plot->id]);
		}else{
			$this->db->executeChangeRaw($this->sqlRemovePlotById, [":level" => $plot->levelName, ":X" => $plot->X, ":Z" => $plot->Z]);
		}
		$plot = new Plot($plot->levelName, $plot->X, $plot->Z);
		$this->cachePlot($plot);
		return true;
	}

	/**
	 * @param string $levelName
	 * @param int $X
	 * @param int $Z
	 *
	 * @return Plot
	 */
	public function getPlot(string $levelName, int $X, int $Z) : Plot {
		if(($plot = $this->getPlotFromCache($levelName, $X, $Z)) != null) {
			return $plot;
		}
		$this->db->executeSelectRaw($this->sqlGetPlot, [":level" => $levelName, ":X" => $X, ":Z" => $Z], function($val, $columns) use ($levelName, $X, $Z, &$plot){
			if(empty($val["helpers"])) {
				$helpers = [];
			}else{
				$helpers = explode(",", (string) $val["helpers"]);
			}
			if(empty($val["denied"])) {
				$denied = [];
			}else{
				$denied = explode(",", (string) $val["denied"]);
			}
			$pvp = is_numeric($val["pvp"]) ? (bool)$val["pvp"] : null;
			$plot = new Plot($levelName, $X, $Z, (string) $val["name"], (string) $val["owner"], $helpers, $denied, (string) $val["biome"], $pvp, (int) $val["id"]);
			$this->cachePlot($plot);
		});
		$plot = new Plot($levelName, $X, $Z);
		$this->cachePlot($plot);
		return $plot;
	}

	/**
	 * @param string $owner
	 * @param string $levelName
	 *
	 * @return array
	 */
	public function getPlotsByOwner(string $owner, string $levelName = "") : array {
		$plots = [];
		$func = function($val, $columns) use (&$plots) {
			$helpers = explode(",", (string) $val["helpers"]);
			$denied = explode(",", (string) $val["denied"]);
			$pvp = is_numeric($val["pvp"]) ? (bool)$val["pvp"] : null;
			$plots[] = new Plot((string) $val["level"], (int) $val["X"], (int) $val["Z"], (string) $val["name"], (string) $val["owner"], $helpers, $denied, (string) $val["biome"], $pvp, (int) $val["id"]);

			$plots = array_filter($plots, function($plot) {
				return $this->plugin->isLevelLoaded($plot->levelName);
			});
			// Sort plots by level
			usort($plots, function($plot1, $plot2) {
				return strcmp($plot1->levelName, $plot2->levelName);
			});
		};
		if(empty($levelName)) {
			$this->db->executeSelectRaw($this->sqlGetPlotsByOwner, [":owner" => $owner], $func);
		}else{
			$this->db->executeSelectRaw($this->sqlGetPlotsByOwnerAndLevel, [":owner" => $owner, ":level" => $levelName], $func);
		}
		return $plots;
	}

	/**
	 * @param string $levelName
	 * @param int $limitXZ
	 *
	 * @return Plot|null
	 */
	public function getNextFreePlot(string $levelName, int $limitXZ = 0) : ?Plot {
		for($i = 0; $limitXZ <= 0 or $i < $limitXZ; $i++) {
			$plots = [];
			$this->db->executeSelectRaw($this->sqlGetExistingXZ, [":level" => $levelName, ":number" => $i], function($val, $columns) use (&$plots) {
				$plots[$val[0]][$val[1]] = true;
			});
			if(count($plots) === max(1, 8 * $i)) {
				continue;
			}
			if($ret = self::findEmptyPlotSquared(0, $i, $plots)) {
				[$X, $Z] = $ret;
				$plot = new Plot($levelName, $X, $Z);
				$this->cachePlot($plot);
				return $plot;
			}
			for($a = 1; $a < $i; $a++) {
				if($ret = self::findEmptyPlotSquared($a, $i, $plots)) {
					[$X, $Z] = $ret;
					$plot = new Plot($levelName, $X, $Z);
					$this->cachePlot($plot);
					return $plot;
				}
			}
			if($ret = self::findEmptyPlotSquared($i, $i, $plots)) {
				[$X, $Z] = $ret;
				$plot = new Plot($levelName, $X, $Z);
				$this->cachePlot($plot);
				return $plot;
			}
		}
		return null;
	}

	public function close() : void {
		$this->db->close();
		$this->plugin->getLogger()->debug("SQLite database closed!");
	}
}