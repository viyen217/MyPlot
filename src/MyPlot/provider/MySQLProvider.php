<?php
declare(strict_types=1);
namespace MyPlot\provider;

use MyPlot\MyPlot;
use MyPlot\Plot;
use poggit\libasynql\base\DataConnectorImpl;
use poggit\libasynql\DataConnector;
use poggit\libasynql\libasynql;

class MySQLProvider extends DataProvider {
	/** @var MyPlot $plugin */
	protected $plugin;
	/** @var DataConnector $db */
	protected $db;
	/** @var array $settings */
	protected $settings;
	/** @var string $sqlGetPlot */
	protected $sqlGetPlot = "SELECT id, name, owner, helpers, denied, biome, pvp FROM plots WHERE level = ? AND X = ? AND Z = ?;";
	/** @var string $sqlSavePlot */
	protected $sqlSavePlot = "INSERT INTO plots (`id`, `level`, `X`, `Z`, `name`, `owner`, `helpers`, `denied`, `biome`, `pvp`) VALUES
			((SELECT id FROM plots p WHERE p.level = ? AND X = ? AND Z = ?),?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE 
			name = VALUES(name), owner = VALUES(owner), helpers = VALUES(helpers), denied = VALUES(denied), biome = VALUES(biome), pvp = VALUES(pvp);";
	/** @var string $sqlSavePlotById */
	protected $sqlSavePlotById = "UPDATE plots SET id = ?, level = ?, X = ?, Z = ?, name = ?, owner = ?, helpers = ?, denied = ?, biome = ?, pvp = ? WHERE id = VALUES(id);";
	/** @var string $sqlRemovePlot */
	protected $sqlRemovePlot = "DELETE FROM plots WHERE id = ?;";
	/** @var string $sqlRemovePlotById */
	protected $sqlRemovePlotById = "DELETE FROM plots WHERE level = ? AND X = ? AND Z = ?;";
	/** @var string $sqlGetPlotsByOwner */
	protected $sqlGetPlotsByOwner = "SELECT * FROM plots WHERE owner = ?;";
	/** @var string $sqlGetPlotsByOwnerAndLevel */
	protected $sqlGetPlotsByOwnerAndLevel = "SELECT * FROM plots WHERE owner = ? AND level = ?;";
	/** @var string $sqlGetExistingXZ */
	protected $sqlGetExistingXZ = "SELECT X, Z FROM plots WHERE (level = ? AND ((abs(X) = ? AND abs(Z) <= ?) OR (abs(Z) = ? AND abs(X) <= ?)));";

	/**
	 * MySQLProvider constructor.
	 *
	 * @param MyPlot $plugin
	 * @param int $cacheSize
	 * @param array $settings
	 */
	public function __construct(MyPlot $plugin, int $cacheSize = 0, array $settings = []) {
		ini_set("mysqli.reconnect", "1");
		ini_set('mysqli.allow_persistent', "1");
		ini_set('mysql.connect_timeout', "300");
		ini_set('default_socket_timeout', "300");
		$this->plugin = $plugin;
		parent::__construct($plugin, $cacheSize);
		$this->settings = $settings;
		$this->db = libasynql::create($plugin, [
			"type" => "mysql",
			"mysql" => [
				"host" => $settings['Host'],
				"username" => $settings['Username'],
				"password" => $settings['Password'],
				"schema" => $settings['DatabaseName'],
				"port" => $settings['Port']
			],
			"worker-limit" => 2
		], [
			"mysql" => []
		]);
		$this->db->executeGenericRaw("CREATE TABLE IF NOT EXISTS plots (id INT PRIMARY KEY AUTO_INCREMENT, level TEXT, X INT, Z INT, name TEXT, owner TEXT, helpers TEXT, denied TEXT, biome TEXT, pvp INT);");
		$this->db->executeGenericRaw("ALTER TABLE plots ADD COLUMN pvp INT AFTER biome;");
		$this->plugin->getLogger()->debug("MySQL data provider registered");
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
			$this->db->executeChangeRaw($this->sqlSavePlotById, [$plot->id, $plot->levelName, $plot->X, $plot->Z, $plot->name, $plot->owner, $helpers, $denied, $plot->biome, $plot->pvp]);
		}else{
			$this->db->executeInsertRaw($this->sqlSavePlot, [$plot->levelName, $plot->X, $plot->Z, $plot->levelName, $plot->X, $plot->Z, $plot->name, $plot->owner, $helpers, $denied, $plot->biome, $plot->pvp]);
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
			$this->db->executeChangeRaw($this->sqlRemovePlot, [$plot->id]);
		}else{
			$this->db->executeChangeRaw($this->sqlRemovePlotById, [$plot->levelName, $plot->X, $plot->Z]);
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
		$this->db->executeSelectRaw($this->sqlGetPlot, [$levelName, $X, $Z], function($val, $columns) use ($levelName, $X, $Z, &$plot){
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
			$this->db->executeSelectRaw($this->sqlGetPlotsByOwner, [$owner], $func);
		}else{
			$this->db->executeSelectRaw($this->sqlGetPlotsByOwnerAndLevel, [$owner, $levelName], $func);
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
		$i = 0;
		for(; $limitXZ <= 0 or $i < $limitXZ; $i++) {
			$plots = [];
			$this->db->executeSelectRaw($this->sqlGetExistingXZ, [$levelName, $i, $i, $i, $i], function($val, $columns) use (&$plots) {
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
		$this->plugin->getLogger()->debug("MySQL database closed!");
	}
}
